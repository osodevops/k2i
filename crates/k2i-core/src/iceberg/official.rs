//! Official Iceberg metadata commit support.
//!
//! This module uses Apache iceberg-rust transaction APIs to produce real
//! metadata JSON, manifest lists, and manifest files. The existing
//! `CatalogOperations` implementations remain useful for lifecycle and health
//! checks, but append commits that need DuckDB `iceberg_scan` compatibility
//! should go through this path.
//!
//! ## Adapter over the official REST catalog client
//!
//! [`OfficialRestCommitter`] implements [`CatalogOperations`] by delegating all
//! methods — including `update_schema` — to the underlying
//! `iceberg_catalog_rest::RestCatalog` and the official `Transaction` API from
//! `iceberg` 0.10.0+.

use crate::config::{CatalogType, CredentialType, IcebergConfig};
use crate::iceberg::factory::{
    CatalogHealth, CatalogOperations, DataFileInfo, SchemaFieldInfo, SnapshotCommit,
    SnapshotCommitResult, TableInfo, TableSchema,
};
use crate::{Error, IcebergError, Result};
use ::iceberg::{Catalog, CatalogBuilder};
use async_trait::async_trait;
use iceberg_catalog_rest::{
    RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

const RECENT_MANIFEST_LIST_LIMIT: usize = 64;

/// Result of a real Iceberg append commit.
#[derive(Debug, Clone)]
pub struct OfficialCommitResult {
    /// Committed snapshot ID from the Iceberg table metadata.
    pub snapshot_id: i64,
    /// Real manifest-list path for the committed snapshot.
    pub manifest_list_path: String,
}

/// REST-catalog backed official Iceberg committer.
///
/// All [`CatalogOperations`] methods delegate to the underlying `RestCatalog`
/// through the official `iceberg-rust` transaction APIs.
pub struct OfficialRestCommitter {
    catalog: RestCatalog,
    warehouse_path: String,
    /// Bounded commit metadata cache used to preserve the real manifest-list
    /// path through the catalog abstraction without changing its result type.
    recent_manifest_lists: Mutex<VecDeque<(i64, String)>>,
}

impl OfficialRestCommitter {
    /// Returns true when this config should use the official REST commit path.
    pub fn is_enabled(config: &IcebergConfig) -> bool {
        config.catalog_type == CatalogType::Rest && config.rest_uri.is_some()
    }

    /// Build a committer from K2I Iceberg config.
    pub async fn new(config: &IcebergConfig) -> Result<Self> {
        let wh = warehouse_uri(&config.warehouse_path)?;

        let mut props = HashMap::from([
            (
                REST_CATALOG_PROP_URI.to_string(),
                config
                    .rest_uri
                    .as_ref()
                    .ok_or_else(|| {
                        Error::Config("official REST committer requires rest_uri".into())
                    })?
                    .clone(),
            ),
            (REST_CATALOG_PROP_WAREHOUSE.to_string(), wh),
        ]);

        apply_rest_auth_props(config, &mut props)?;
        apply_file_io_props(config, &mut props);

        for (key, value) in &config.rest.custom_headers {
            props.insert(format!("header.{}", key), value.clone());
        }

        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
            .load("k2i", props)
            .await
            .map_err(map_iceberg_error)?;

        Ok(Self {
            catalog,
            warehouse_path: config.warehouse_path.clone(),
            recent_manifest_lists: Mutex::new(VecDeque::new()),
        })
    }

    /// Commit existing Parquet files to an existing Iceberg table.
    pub async fn commit_append(
        &self,
        namespace: &str,
        table: &str,
        files: Vec<DataFileInfo>,
        summary: HashMap<String, String>,
    ) -> Result<OfficialCommitResult> {
        commit_append_with_catalog(
            &self.catalog,
            &self.warehouse_path,
            namespace,
            table,
            files,
            summary,
        )
        .await
    }

    fn remember_manifest_list(&self, result: &OfficialCommitResult) {
        let mut recent = self.recent_manifest_lists.lock();
        if let Some(index) = recent
            .iter()
            .position(|(snapshot_id, _)| *snapshot_id == result.snapshot_id)
        {
            recent.remove(index);
        }
        recent.push_back((result.snapshot_id, result.manifest_list_path.clone()));
        while recent.len() > RECENT_MANIFEST_LIST_LIMIT {
            recent.pop_front();
        }
    }
}

#[async_trait]
impl CatalogOperations for OfficialRestCommitter {
    async fn health_check(&self) -> Result<CatalogHealth> {
        let start = Instant::now();

        // Use list_namespaces as a health probe — this implicitly validates
        // prefix negotiation + auth in one round-trip.  The official client
        // caches prefix resolution internally via OnceCell, so a subsequent
        // health check never re-resolves /v1/config.
        match self.catalog.list_namespaces(None).await {
            Ok(_) => {
                let elapsed = start.elapsed().as_millis() as u64;
                Ok(CatalogHealth {
                    is_healthy: true,
                    response_time_ms: elapsed,
                    message: Some("REST catalog is healthy".to_string()),
                    catalog_type: CatalogType::Rest,
                })
            }
            Err(e) => {
                let elapsed = start.elapsed().as_millis() as u64;
                Ok(CatalogHealth {
                    is_healthy: false,
                    response_time_ms: elapsed,
                    message: Some(format!("Catalog health check failed: {}", e)),
                    catalog_type: CatalogType::Rest,
                })
            }
        }
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let namespaces = self
            .catalog
            .list_namespaces(None)
            .await
            .map_err(map_iceberg_error)?;
        Ok(namespaces
            .into_iter()
            .map(|ns| ns.inner().join("."))
            .collect())
    }

    async fn namespace_exists(&self, namespace: &str) -> Result<bool> {
        let ident = namespace_ident(namespace)?;
        self.catalog
            .namespace_exists(&ident)
            .await
            .map_err(map_iceberg_error)
    }

    async fn create_namespace(&self, namespace: &str) -> Result<()> {
        let ident = namespace_ident(namespace)?;
        match self.catalog.create_namespace(&ident, HashMap::new()).await {
            Ok(_) => {
                info!(namespace = %namespace, "Created namespace");
                Ok(())
            }
            Err(e) if e.kind() == ::iceberg::ErrorKind::NamespaceAlreadyExists => {
                debug!(namespace = %namespace, "Namespace already exists");
                Ok(())
            }
            Err(e) => Err(map_iceberg_error(e)),
        }
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let ident = namespace_ident(namespace)?;
        let tables = self
            .catalog
            .list_tables(&ident)
            .await
            .map_err(map_iceberg_error)?;
        Ok(tables.into_iter().map(|t| t.name).collect())
    }

    async fn table_exists(&self, namespace: &str, table: &str) -> Result<bool> {
        let ident = table_ident(namespace, table)?;
        self.catalog
            .table_exists(&ident)
            .await
            .map_err(map_iceberg_error)
    }

    async fn load_table(&self, namespace: &str, table: &str) -> Result<TableInfo> {
        let ident = table_ident(namespace, table)?;
        let loaded = self
            .catalog
            .load_table(&ident)
            .await
            .map_err(map_iceberg_error)?;
        Ok(table_info_from(namespace, table, &loaded))
    }

    async fn create_table(
        &self,
        namespace: &str,
        table: &str,
        schema: &TableSchema,
    ) -> Result<TableInfo> {
        let ns_ident = namespace_ident(namespace)?;
        let iceberg_schema = table_schema_to_iceberg_schema(schema)?;
        let table_creation = ::iceberg::TableCreation::builder()
            .name(table.to_string())
            .schema(iceberg_schema)
            .build();
        let created = self
            .catalog
            .create_table(&ns_ident, table_creation)
            .await
            .map_err(map_iceberg_error)?;
        info!(namespace = %namespace, table = %table, "Created table");
        Ok(table_info_from(namespace, table, &created))
    }

    async fn current_snapshot_id(&self, namespace: &str, table: &str) -> Result<Option<i64>> {
        let ident = table_ident(namespace, table)?;
        let loaded = self
            .catalog
            .load_table(&ident)
            .await
            .map_err(map_iceberg_error)?;
        Ok(loaded.metadata().current_snapshot_id())
    }

    async fn commit_snapshot(
        &self,
        namespace: &str,
        table: &str,
        commit: SnapshotCommit,
    ) -> Result<SnapshotCommitResult> {
        // NOTE: `commit.expected_snapshot_id` is intentionally not used here.
        // Optimistic concurrency (CAS) is enforced by the official
        // `Transaction` inside `commit_append_with_catalog`, which loads the
        // current table metadata and attaches a `RefSnapshotIdMatch`
        // requirement that the REST catalog validates at commit time. The
        // caller-supplied `expected_snapshot_id` (derived from the metadata
        // cache) would only duplicate that check.
        //
        // This path performs a `fast_append` and cannot remove files. Ingestion
        // never requests removals, but maintenance compaction does — silently
        // dropping them would leave compacted rows duplicated, so removals are
        // rejected with a hard error.
        if !commit.files_to_remove.is_empty() {
            return Err(Error::Iceberg(IcebergError::SnapshotCommit(format!(
                "official REST catalog append commits do not support removing files; \
                 refusing to ignore {} requested removals",
                commit.files_to_remove.len()
            ))));
        }

        let files_added = commit.files_to_add.len();

        let result = commit_append_with_catalog(
            &self.catalog,
            &self.warehouse_path,
            namespace,
            table,
            commit.files_to_add,
            commit.summary,
        )
        .await?;
        self.remember_manifest_list(&result);

        Ok(SnapshotCommitResult {
            snapshot_id: result.snapshot_id,
            committed_at: chrono::Utc::now(),
            files_added,
            files_removed: 0,
        })
    }

    fn manifest_list_path_for_snapshot(&self, snapshot_id: i64) -> Option<String> {
        self.recent_manifest_lists
            .lock()
            .iter()
            .find(|(candidate, _)| *candidate == snapshot_id)
            .map(|(_, path)| path.clone())
    }

    /// Additive-only schema evolution through the official `Transaction` API.
    ///
    /// `expected_schema_id` is intentionally unused: the transaction loads the
    /// current table metadata and the REST catalog validates the commit
    /// requirements derived from it, so a caller-side schema-id assertion
    /// would only duplicate that check. Fields whose names already exist are
    /// skipped — callers are expected to run `diff_table_schema` first, which
    /// rejects type or requiredness changes as breaking.
    async fn update_schema(
        &self,
        namespace: &str,
        table: &str,
        schema: &TableSchema,
        _expected_schema_id: Option<i32>,
    ) -> Result<TableInfo> {
        use ::iceberg::transaction::{ApplyTransactionAction, Transaction};

        let ident = table_ident(namespace, table)?;
        let loaded = self
            .catalog
            .load_table(&ident)
            .await
            .map_err(map_iceberg_error)?;

        let current_schema = loaded.metadata().current_schema();
        let current_names: std::collections::HashSet<_> = current_schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect();

        let tx = Transaction::new(&loaded);
        let mut update = tx.update_schema();
        let mut additions = 0;

        for field in &schema.fields {
            if current_names.contains(field.name.as_str()) {
                continue;
            }

            let field_type = parse_iceberg_type(&field.field_type)?;
            let add = if field.required {
                return Err(Error::Iceberg(IcebergError::SchemaEvolution(format!(
                    "cannot add required column {} via additive schema evolution",
                    field.name
                ))));
            } else {
                match &field.doc {
                    Some(doc) => ::iceberg::transaction::AddColumn::builder()
                        .name(field.name.clone())
                        .field_type(field_type)
                        .required(false)
                        .doc(doc.clone())
                        .build(),
                    None => ::iceberg::transaction::AddColumn::builder()
                        .name(field.name.clone())
                        .field_type(field_type)
                        .required(false)
                        .build(),
                }
            };

            update = update.add_column(add);
            additions += 1;
        }

        if additions == 0 {
            return Ok(table_info_from(namespace, table, &loaded));
        }

        let tx = update.apply(tx).map_err(map_iceberg_error)?;
        let committed = tx.commit(&self.catalog).await.map_err(map_iceberg_error)?;

        info!(
            namespace = %namespace,
            table = %table,
            additions,
            "Schema update committed via official REST transaction path"
        );

        Ok(table_info_from(namespace, table, &committed))
    }

    fn catalog_type(&self) -> CatalogType {
        CatalogType::Rest
    }

    fn warehouse_path(&self) -> &str {
        &self.warehouse_path
    }

    async fn close(&self) -> Result<()> {
        info!("Official REST committer closed");
        Ok(())
    }
}

/// Convert a dot-separated namespace string to an Iceberg `NamespaceIdent`.
fn namespace_ident(namespace: &str) -> Result<::iceberg::NamespaceIdent> {
    let parts: Vec<&str> = namespace.split('.').filter(|p| !p.is_empty()).collect();
    ::iceberg::NamespaceIdent::from_strs(parts).map_err(map_iceberg_error)
}

/// Convert an Iceberg type to the JSON string format used by the protobuf decoder.
///
/// Primitive types become plain strings (e.g. `"double"`), while nested types
/// produce full JSON objects so that round-tripping through `TableSchema` and
/// `parse_iceberg_type` preserves the exact representation.
fn iceberg_type_to_json_string(ty: &::iceberg::spec::Type) -> String {
    use ::iceberg::spec::Type;
    match ty {
        Type::Primitive(p) => {
            // Primitives use the same lowercase names as iceberg_type_value.
            let s = match p {
                ::iceberg::spec::PrimitiveType::Boolean => "boolean",
                ::iceberg::spec::PrimitiveType::Int => "int",
                ::iceberg::spec::PrimitiveType::Long => "long",
                ::iceberg::spec::PrimitiveType::Float => "float",
                ::iceberg::spec::PrimitiveType::Double => "double",
                ::iceberg::spec::PrimitiveType::Date => "date",
                ::iceberg::spec::PrimitiveType::Time => "time",
                ::iceberg::spec::PrimitiveType::Timestamp => "timestamp",
                ::iceberg::spec::PrimitiveType::Timestamptz => "timestamptz",
                ::iceberg::spec::PrimitiveType::TimestampNs => "timestamp_ns",
                ::iceberg::spec::PrimitiveType::TimestamptzNs => "timestamptz_ns",
                ::iceberg::spec::PrimitiveType::String => "string",
                ::iceberg::spec::PrimitiveType::Uuid => "uuid",
                ::iceberg::spec::PrimitiveType::Fixed(len) => {
                    return format!("fixed[{}]", len);
                }
                ::iceberg::spec::PrimitiveType::Binary => "binary",
                ::iceberg::spec::PrimitiveType::Decimal { precision, scale } => {
                    return format!("decimal({},{})", precision, scale);
                }
            };
            s.to_string()
        }
        Type::Struct(s) => {
            let fields: Vec<serde_json::Value> = s
                .fields()
                .iter()
                .map(|f| {
                    serde_json::json!({
                    "id": f.id,
                    "name": f.name,
                    "required": f.required,
                    "type": serde_json::from_str::<serde_json::Value>(
                    &iceberg_type_to_json_string(&f.field_type)
                    )
                    .unwrap_or_else(|_| serde_json::Value::String(
                    iceberg_type_to_json_string(&f.field_type)
                    )),
                    "doc": f.doc,
                    })
                })
                .collect();
            serde_json::json!({
                "type": "struct",
                "fields": fields,
            })
            .to_string()
        }
        Type::List(l) => {
            let element = iceberg_type_to_json_string(&l.element_field.field_type);
            let element_value = serde_json::from_str::<serde_json::Value>(&element)
                .unwrap_or(serde_json::Value::String(element));
            serde_json::json!({
                "type": "list",
                "element-id": l.element_field.id,
                "element": element_value,
                "element-required": l.element_field.required,
            })
            .to_string()
        }
        Type::Map(m) => {
            let key = iceberg_type_to_json_string(&m.key_field.field_type);
            let value = iceberg_type_to_json_string(&m.value_field.field_type);
            let key_value = serde_json::from_str::<serde_json::Value>(&key)
                .unwrap_or(serde_json::Value::String(key));
            let value_value = serde_json::from_str::<serde_json::Value>(&value)
                .unwrap_or(serde_json::Value::String(value));
            serde_json::json!({
                "type": "map",
                "key-id": m.key_field.id,
                "key": key_value,
                "value-id": m.value_field.id,
                "value": value_value,
                "value-required": m.value_field.required,
            })
            .to_string()
        }
    }
}

/// Convert an official Iceberg `Schema` back into K2I's `TableSchema`.
fn iceberg_schema_to_table_schema(schema: &::iceberg::spec::Schema) -> TableSchema {
    TableSchema {
        schema_id: schema.schema_id(),
        fields: schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| SchemaFieldInfo {
                id: field.id,
                name: field.name.clone(),
                field_type: iceberg_type_to_json_string(&field.field_type),
                required: field.required,
                doc: field.doc.clone(),
            })
            .collect(),
    }
}

/// Build a `TableInfo` from an official Iceberg `Table`.
fn table_info_from(namespace: &str, table: &str, table_ref: &::iceberg::table::Table) -> TableInfo {
    let metadata = table_ref.metadata();
    let schema = iceberg_schema_to_table_schema(metadata.current_schema());
    TableInfo {
        namespace: namespace.to_string(),
        name: table.to_string(),
        location: metadata.location().to_string(),
        current_snapshot_id: metadata.current_snapshot_id(),
        schema,
        properties: metadata.properties().clone(),
    }
}

/// Commit existing Parquet files with any official Iceberg catalog.
pub async fn commit_append_with_catalog(
    catalog: &dyn ::iceberg::Catalog,
    warehouse_path: &str,
    namespace: &str,
    table: &str,
    files: Vec<DataFileInfo>,
    mut summary: HashMap<String, String>,
) -> Result<OfficialCommitResult> {
    use ::iceberg::transaction::{ApplyTransactionAction, Transaction};
    if files.is_empty() {
        return Err(Error::Iceberg(IcebergError::SnapshotCommit(
            "cannot append an empty file set".into(),
        )));
    }

    // Remove "operation" from the summary HashMap before passing to the
    // official iceberg-rust crate.  The crate's Summary struct has `operation`
    // as a named field plus `#[serde(flatten)] additional_properties`.  If
    // "operation" is left in the HashMap, it gets serialized twice (once from
    // the struct field, once from the flattened map), causing "duplicate field
    // `operation`" errors in catalogs like Lakekeeper.
    //
    // See: https://github.com/apache/iceberg/issues/9837
    summary.remove("operation");

    let ident = table_ident(namespace, table)?;
    let loaded = catalog
        .load_table(&ident)
        .await
        .map_err(map_iceberg_error)?;
    let spec_id = loaded.metadata().default_partition_spec_id();
    let data_files = files
        .iter()
        .map(|file| data_file_from_info(file, warehouse_path, spec_id))
        .collect::<Result<Vec<_>>>()?;

    let tx = Transaction::new(&loaded);
    let action = tx
        .fast_append()
        .set_snapshot_properties(summary)
        .add_data_files(data_files);
    let tx = action.apply(tx).map_err(map_iceberg_error)?;
    let committed = tx.commit(catalog).await.map_err(map_iceberg_error)?;
    let snapshot = committed.metadata().current_snapshot().ok_or_else(|| {
        Error::Iceberg(IcebergError::SnapshotCommit(
            "append commit completed without a current snapshot".into(),
        ))
    })?;

    Ok(OfficialCommitResult {
        snapshot_id: snapshot.snapshot_id(),
        manifest_list_path: snapshot.manifest_list().to_string(),
    })
}

/// Convert a K2I table schema to an official Iceberg schema.
pub fn table_schema_to_iceberg_schema(schema: &TableSchema) -> Result<::iceberg::spec::Schema> {
    let fields = schema
        .fields
        .iter()
        .map(nested_field_from_info)
        .collect::<Result<Vec<_>>>()?;

    ::iceberg::spec::Schema::builder()
        .with_schema_id(schema.schema_id)
        .with_fields(fields)
        .build()
        .map_err(map_iceberg_error)
}

fn nested_field_from_info(field: &SchemaFieldInfo) -> Result<::iceberg::spec::NestedFieldRef> {
    let field_type = parse_iceberg_type(&field.field_type)?;
    let mut nested = if field.required {
        ::iceberg::spec::NestedField::required(field.id, &field.name, field_type)
    } else {
        ::iceberg::spec::NestedField::optional(field.id, &field.name, field_type)
    };
    nested.doc = field.doc.clone();
    Ok(Arc::new(nested))
}

fn parse_iceberg_type(field_type: &str) -> Result<::iceberg::spec::Type> {
    use ::iceberg::spec::{PrimitiveType, Type};

    let trimmed = field_type.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return serde_json::from_str::<Type>(trimmed)
            .map_err(|e| Error::Iceberg(IcebergError::SchemaEvolution(e.to_string())));
    }

    let lower = trimmed.to_ascii_lowercase();
    let primitive = match lower.as_str() {
        "boolean" | "bool" => PrimitiveType::Boolean,
        "int" | "integer" => PrimitiveType::Int,
        "long" | "bigint" => PrimitiveType::Long,
        "float" => PrimitiveType::Float,
        "double" => PrimitiveType::Double,
        "date" => PrimitiveType::Date,
        "time" => PrimitiveType::Time,
        "timestamp" => PrimitiveType::Timestamp,
        "timestamptz" => PrimitiveType::Timestamptz,
        "timestamp_ns" => PrimitiveType::TimestampNs,
        "timestamptz_ns" => PrimitiveType::TimestamptzNs,
        "string" | "utf8" => PrimitiveType::String,
        "uuid" => PrimitiveType::Uuid,
        "binary" => PrimitiveType::Binary,
        _ if lower.starts_with("decimal(") && lower.ends_with(')') => {
            let args = &lower["decimal(".len()..lower.len() - 1];
            let (precision, scale) = args.split_once(',').ok_or_else(|| {
                Error::Iceberg(IcebergError::SchemaEvolution(format!(
                    "invalid decimal type: {}",
                    field_type
                )))
            })?;
            PrimitiveType::Decimal {
                precision: precision.trim().parse().map_err(|e| {
                    Error::Iceberg(IcebergError::SchemaEvolution(format!(
                        "invalid decimal precision {}: {}",
                        precision, e
                    )))
                })?,
                scale: scale.trim().parse().map_err(|e| {
                    Error::Iceberg(IcebergError::SchemaEvolution(format!(
                        "invalid decimal scale {}: {}",
                        scale, e
                    )))
                })?,
            }
        }
        _ if lower.starts_with("fixed[") && lower.ends_with(']') => {
            let len = &lower["fixed[".len()..lower.len() - 1];
            PrimitiveType::Fixed(len.parse().map_err(|e| {
                Error::Iceberg(IcebergError::SchemaEvolution(format!(
                    "invalid fixed length {}: {}",
                    len, e
                )))
            })?)
        }
        _ => {
            return Err(Error::Iceberg(IcebergError::SchemaEvolution(format!(
                "unsupported Iceberg type: {}",
                field_type
            ))));
        }
    };

    Ok(Type::Primitive(primitive))
}

fn data_file_from_info(
    file: &DataFileInfo,
    warehouse_path: &str,
    partition_spec_id: i32,
) -> Result<::iceberg::spec::DataFile> {
    use ::iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};

    let format = match file.file_format.to_ascii_lowercase().as_str() {
        "parquet" => DataFileFormat::Parquet,
        "avro" => DataFileFormat::Avro,
        "orc" => DataFileFormat::Orc,
        other => {
            return Err(Error::Iceberg(IcebergError::SnapshotCommit(format!(
                "unsupported data file format: {}",
                other
            ))));
        }
    };

    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(data_file_uri(&file.file_path, warehouse_path)?)
        .file_format(format)
        .file_size_in_bytes(file.file_size_bytes)
        .record_count(file.record_count)
        .partition_spec_id(partition_spec_id)
        .partition(Struct::empty())
        .build()
        .map_err(|e| Error::Iceberg(IcebergError::SnapshotCommit(e.to_string())))
}

fn table_ident(namespace: &str, table: &str) -> Result<::iceberg::TableIdent> {
    let parts = namespace
        .split('.')
        .filter(|part| !part.is_empty())
        .chain(std::iter::once(table))
        .collect::<Vec<_>>();
    ::iceberg::TableIdent::from_strs(parts).map_err(map_iceberg_error)
}

fn warehouse_uri(warehouse_path: &str) -> Result<String> {
    if warehouse_path.contains("://") {
        return Ok(warehouse_path.trim_end_matches('/').to_string());
    }

    let path = Path::new(warehouse_path);
    let absolute = if path.is_absolute() {
        PathBuf::from(path)
    } else {
        std::env::current_dir()?.join(path)
    };

    Ok(format!("file://{}", absolute.to_string_lossy())
        .trim_end_matches('/')
        .to_string())
}

fn data_file_uri(file_path: &str, warehouse_path: &str) -> Result<String> {
    if file_path.contains("://") {
        return Ok(file_path.to_string());
    }

    let path = Path::new(file_path);
    if path.is_absolute() {
        return Ok(format!("file://{}", path.to_string_lossy()));
    }

    let warehouse = warehouse_uri(warehouse_path)?;
    Ok(format!(
        "{}/{}",
        warehouse.trim_end_matches('/'),
        file_path.trim_start_matches('/')
    ))
}

fn apply_rest_auth_props(
    config: &IcebergConfig,
    props: &mut HashMap<String, String>,
) -> Result<()> {
    match config.rest.credential_type {
        CredentialType::None => {}
        CredentialType::Bearer => {
            let token = config.rest.credential.as_ref().ok_or_else(|| {
                Error::Config("REST bearer auth requires iceberg.rest.credential".into())
            })?;
            props.insert("token".to_string(), token.clone());
        }
        CredentialType::OAuth2 => {
            let client_secret = config.rest.oauth2_client_secret.as_ref().ok_or_else(|| {
                Error::Config("REST OAuth2 requires iceberg.rest.oauth2_client_secret".into())
            })?;
            let credential = match &config.rest.oauth2_client_id {
                Some(client_id) => format!("{}:{}", client_id, client_secret),
                None => client_secret.clone(),
            };
            props.insert("credential".to_string(), credential);
            if let Some(endpoint) = &config.rest.oauth2_token_endpoint {
                props.insert("oauth2-server-uri".to_string(), endpoint.clone());
            }
            if let Some(scope) = &config.rest.oauth2_scope {
                props.insert("scope".to_string(), scope.clone());
            }
        }
    }

    Ok(())
}

fn apply_file_io_props(config: &IcebergConfig, props: &mut HashMap<String, String>) {
    if let Some(endpoint) = &config.s3_endpoint {
        props.insert("s3.endpoint".to_string(), endpoint.clone());
    }
    if let Some(access_key) = &config.aws_access_key_id {
        props.insert("s3.access-key-id".to_string(), access_key.clone());
    }
    if let Some(secret_key) = &config.aws_secret_access_key {
        props.insert("s3.secret-access-key".to_string(), secret_key.clone());
    }
    if let Some(region) = &config.aws_region {
        props.insert("s3.region".to_string(), region.clone());
    }
}

fn map_iceberg_error(err: ::iceberg::Error) -> Error {
    let message = err.to_string();
    match err.kind() {
        ::iceberg::ErrorKind::TableNotFound => Error::Iceberg(IcebergError::TableNotFound(message)),
        ::iceberg::ErrorKind::CatalogCommitConflicts => Error::Iceberg(IcebergError::CasConflict {
            expected: -1,
            actual: -1,
        }),
        _ => Error::Iceberg(IcebergError::Other(message)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    use ::iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};

    fn simple_schema() -> TableSchema {
        TableSchema {
            schema_id: 1,
            fields: vec![
                SchemaFieldInfo {
                    id: 1,
                    name: "id".into(),
                    field_type: "long".into(),
                    required: true,
                    doc: None,
                },
                SchemaFieldInfo {
                    id: 2,
                    name: "driver".into(),
                    field_type: "string".into(),
                    required: false,
                    doc: Some("driver name".into()),
                },
            ],
        }
    }

    fn rest_config(rest_uri: &str) -> IcebergConfig {
        toml::from_str(&format!(
            r#"
catalog_type = "rest"
warehouse_path = "/tmp/k2i-official-tests"
database_name = "analytics"
table_name = "events"
rest_uri = {rest_uri:?}
"#
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn rejects_removals_and_keeps_manifest_cache_bounded() {
        let committer = OfficialRestCommitter::new(&rest_config("http://127.0.0.1:1"))
            .await
            .unwrap();

        for snapshot_id in 0..=RECENT_MANIFEST_LIST_LIMIT as i64 {
            committer.remember_manifest_list(&OfficialCommitResult {
                snapshot_id,
                manifest_list_path: format!("metadata/snap-{snapshot_id}.avro"),
            });
        }
        assert_eq!(committer.manifest_list_path_for_snapshot(0), None);
        assert_eq!(
            committer.manifest_list_path_for_snapshot(RECENT_MANIFEST_LIST_LIMIT as i64),
            Some(format!("metadata/snap-{}.avro", RECENT_MANIFEST_LIST_LIMIT))
        );

        let error = committer
            .commit_snapshot(
                "analytics",
                "events",
                SnapshotCommit {
                    expected_snapshot_id: None,
                    files_to_add: vec![],
                    files_to_remove: vec!["data/old.parquet".into()],
                    summary: HashMap::new(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::Iceberg(IcebergError::SnapshotCommit(message))
                if message.contains("refusing to ignore 1 requested removals")
        ));
    }

    #[test]
    fn warehouse_uri_converts_local_path() {
        assert_eq!(warehouse_uri("/tmp/k2i-wh").unwrap(), "file:///tmp/k2i-wh");
        assert_eq!(
            warehouse_uri("s3://warehouse/prefix/").unwrap(),
            "s3://warehouse/prefix"
        );
    }

    #[test]
    fn data_file_uri_prefixes_relative_local_path() {
        assert_eq!(
            data_file_uri("data/f1/part.parquet", "/tmp/k2i-wh").unwrap(),
            "file:///tmp/k2i-wh/data/f1/part.parquet"
        );
        assert_eq!(
            data_file_uri("/tmp/source.parquet", "/tmp/k2i-wh").unwrap(),
            "file:///tmp/source.parquet"
        );
    }

    #[test]
    fn converts_table_schema_to_official_schema() {
        let schema = table_schema_to_iceberg_schema(&simple_schema()).unwrap();
        assert_eq!(schema.schema_id(), 1);
        assert_eq!(schema.highest_field_id(), 2);
        assert_eq!(
            schema.field_by_name("driver").unwrap().doc.as_deref(),
            Some("driver name")
        );
    }

    #[tokio::test]
    async fn commits_append_with_memory_catalog() {
        let temp = tempfile::tempdir().unwrap();
        let warehouse = warehouse_uri(temp.path().to_str().unwrap()).unwrap();
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
            )
            .await
            .unwrap();
        let namespace = NamespaceIdent::from_strs(["f1"]).unwrap();
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = table_schema_to_iceberg_schema(&simple_schema()).unwrap();
        let table_creation = TableCreation::builder()
            .name("derived_state".to_string())
            .location(format!("{}/f1/derived_state", warehouse))
            .schema(schema)
            .build();
        catalog
            .create_table(&namespace, table_creation)
            .await
            .unwrap();

        let result = commit_append_with_catalog(
            &catalog,
            temp.path().to_str().unwrap(),
            "f1",
            "derived_state",
            vec![DataFileInfo {
                file_path: "data/f1/derived_state/part-1.parquet".into(),
                file_size_bytes: 128,
                record_count: 3,
                partition_values: HashMap::new(),
                file_format: "parquet".into(),
            }],
            HashMap::from([("operation".into(), "append".into())]),
        )
        .await
        .unwrap();

        assert!(result.snapshot_id != 0);
        assert!(result.manifest_list_path.contains("/metadata/snap-"));
        assert!(result.manifest_list_path.ends_with(".avro"));

        let table = catalog
            .load_table(&table_ident("f1", "derived_state").unwrap())
            .await
            .expect("committed table metadata should reload after snapshot serialization");
        let summary = table
            .metadata()
            .current_snapshot()
            .expect("committed table should have a current snapshot")
            .summary();
        assert!(
            !summary.additional_properties.contains_key("operation"),
            "`operation` must not be forwarded as a flattened snapshot property"
        );

        let json = serde_json::to_string(summary).unwrap();
        let round_tripped: ::iceberg::spec::Summary =
            serde_json::from_str(&json).expect("snapshot summary should round-trip as JSON");
        assert_eq!(round_tripped.operation, ::iceberg::spec::Operation::Append);
    }

    #[test]
    fn test_namespace_ident() {
        let ident = namespace_ident("db.schema").unwrap();
        assert_eq!(ident.inner(), vec!["db", "schema"]);
    }

    #[test]
    fn test_namespace_ident_single() {
        let ident = namespace_ident("db").unwrap();
        assert_eq!(ident.inner(), vec!["db"]);
    }

    #[test]
    fn test_iceberg_schema_to_table_schema_roundtrip() {
        let original = simple_schema();
        let official = table_schema_to_iceberg_schema(&original).unwrap();
        let back = iceberg_schema_to_table_schema(&official);
        assert_eq!(back.schema_id, original.schema_id);
        assert_eq!(back.fields.len(), original.fields.len());
        assert_eq!(back.fields[0].name, original.fields[0].name);
        assert_eq!(back.fields[0].field_type, original.fields[0].field_type);
    }

    // ---- iceberg_type_to_json_string unit tests ----

    #[test]
    fn iceberg_type_to_json_all_primitives() {
        use ::iceberg::spec::{PrimitiveType, Type};

        let cases = vec![
            (Type::Primitive(PrimitiveType::Boolean), "boolean"),
            (Type::Primitive(PrimitiveType::Int), "int"),
            (Type::Primitive(PrimitiveType::Long), "long"),
            (Type::Primitive(PrimitiveType::Float), "float"),
            (Type::Primitive(PrimitiveType::Double), "double"),
            (Type::Primitive(PrimitiveType::Date), "date"),
            (Type::Primitive(PrimitiveType::Time), "time"),
            (Type::Primitive(PrimitiveType::Timestamp), "timestamp"),
            (Type::Primitive(PrimitiveType::Timestamptz), "timestamptz"),
            (Type::Primitive(PrimitiveType::TimestampNs), "timestamp_ns"),
            (
                Type::Primitive(PrimitiveType::TimestamptzNs),
                "timestamptz_ns",
            ),
            (Type::Primitive(PrimitiveType::String), "string"),
            (Type::Primitive(PrimitiveType::Uuid), "uuid"),
            (Type::Primitive(PrimitiveType::Binary), "binary"),
            (Type::Primitive(PrimitiveType::Fixed(16)), "fixed[16]"),
            (
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 10,
                    scale: 2,
                }),
                "decimal(10,2)",
            ),
        ];

        for (ty, expected) in cases {
            assert_eq!(
                iceberg_type_to_json_string(&ty),
                expected,
                "mismatch for {:?}",
                ty
            );
        }
    }

    #[test]
    fn iceberg_type_to_json_struct_produces_json_object() {
        use ::iceberg::spec::{NestedField, PrimitiveType, StructType, Type};
        use std::sync::Arc;

        let struct_type = Type::Struct(StructType::new(vec![
            Arc::new(NestedField::required(
                1,
                "speed",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::optional(
                2,
                "unit",
                Type::Primitive(PrimitiveType::String),
            )),
        ]));

        let json_str = iceberg_type_to_json_string(&struct_type);

        // Must produce valid JSON, not the Display format "struct<...>"
        let parsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("must be valid JSON");
        assert_eq!(parsed["type"], "struct");
        assert!(parsed["fields"].is_array());
        let fields = parsed["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0]["name"], "speed");
        assert_eq!(fields[0]["type"], "double");
        assert_eq!(fields[1]["name"], "unit");
        assert_eq!(fields[1]["type"], "string");
    }

    #[test]
    fn iceberg_type_to_json_list_produces_json_object() {
        use ::iceberg::spec::{ListType, NestedField, PrimitiveType, Type};
        use std::sync::Arc;

        let list_type = Type::List(ListType::new(Arc::new(NestedField::list_element(
            1,
            Type::Primitive(PrimitiveType::String),
            false,
        ))));

        let json_str = iceberg_type_to_json_string(&list_type);

        let parsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("must be valid JSON");
        assert_eq!(parsed["type"], "list");
        assert_eq!(parsed["element"], "string");
        assert_eq!(parsed["element-required"], false);
    }

    #[test]
    fn iceberg_type_to_json_map_produces_json_object() {
        use ::iceberg::spec::{MapType, PrimitiveType, Type};

        let map_type = Type::Map(MapType::optional(
            1,
            Type::Primitive(PrimitiveType::String),
            2,
            Type::Primitive(PrimitiveType::Long),
        ));

        let json_str = iceberg_type_to_json_string(&map_type);

        let parsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("must be valid JSON");
        assert_eq!(parsed["type"], "map");
        assert_eq!(parsed["key"], "string");
        assert_eq!(parsed["value"], "long");
        assert_eq!(parsed["value-required"], false);
    }

    #[test]
    fn iceberg_type_to_json_nested_struct_in_list() {
        use ::iceberg::spec::{ListType, NestedField, PrimitiveType, StructType, Type};
        use std::sync::Arc;

        let inner_struct = Type::Struct(StructType::new(vec![
            Arc::new(NestedField::required(
                10,
                "x",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::required(
                11,
                "y",
                Type::Primitive(PrimitiveType::Double),
            )),
        ]));
        let list_of_structs = Type::List(ListType::new(Arc::new(NestedField::list_element(
            5,
            inner_struct,
            true,
        ))));

        let json_str = iceberg_type_to_json_string(&list_of_structs);
        let parsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("must be valid JSON");

        assert_eq!(parsed["type"], "list");
        assert_eq!(parsed["element"]["type"], "struct");
        let element_fields = parsed["element"]["fields"]
            .as_array()
            .expect("struct fields must be an array");
        assert_eq!(element_fields.len(), 2);
        assert_eq!(element_fields[0]["name"], "x");
        assert_eq!(element_fields[1]["name"], "y");
    }

    #[test]
    fn iceberg_type_to_json_not_display_format() {
        // Regression test: the old Display impl produced "struct<field: type>"
        // which parse_iceberg_type could not parse back. The new function must
        // produce valid JSON for nested types.
        use ::iceberg::spec::{NestedField, PrimitiveType, StructType, Type};
        use std::sync::Arc;

        let struct_type = Type::Struct(StructType::new(vec![Arc::new(NestedField::required(
            1,
            "field1",
            Type::Primitive(PrimitiveType::String),
        ))]));

        let result = iceberg_type_to_json_string(&struct_type);
        assert!(
            result.starts_with('{'),
            "nested types must produce JSON, not Display format"
        );
        assert!(
            !result.contains('<'),
            "must not contain angle brackets from Display format"
        );
    }

    #[test]
    fn iceberg_type_to_json_roundtrip_via_parse() {
        // The output of iceberg_type_to_json_string must be parseable by
        // parse_iceberg_type for all nested types.
        use ::iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, StructType, Type};
        use std::sync::Arc;

        let struct_type = Type::Struct(StructType::new(vec![
            Arc::new(NestedField::required(
                1,
                "a",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "b",
                Type::Primitive(PrimitiveType::String),
            )),
        ]));
        let list_type = Type::List(ListType::new(Arc::new(NestedField::list_element(
            3,
            Type::Primitive(PrimitiveType::Double),
            false,
        ))));
        let map_type = Type::Map(MapType::optional(
            4,
            Type::Primitive(PrimitiveType::String),
            5,
            Type::Primitive(PrimitiveType::Long),
        ));

        let fixed_type = Type::Primitive(PrimitiveType::Fixed(16));
        let decimal_type = Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        });

        for ty in [struct_type, list_type, map_type, fixed_type, decimal_type] {
            let json_str = iceberg_type_to_json_string(&ty);
            let parsed = parse_iceberg_type(&json_str)
                .expect("iceberg_type_to_json_string output must be parseable");
            assert_eq!(parsed, ty, "round-trip failed for {:?}", ty);
        }
    }

    #[test]
    fn iceberg_schema_to_table_schema_with_nested_types() {
        // Build an iceberg Schema containing a struct field and a list field,
        // then verify the conversion to TableSchema preserves the nested types
        // in JSON format.
        use ::iceberg::spec::{ListType, NestedField, PrimitiveType, Schema, StructType, Type};
        use std::sync::Arc;

        let struct_field = NestedField::optional(
            2,
            "telemetry",
            Type::Struct(StructType::new(vec![
                Arc::new(NestedField::required(
                    10,
                    "speed",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    11,
                    "gear",
                    Type::Primitive(PrimitiveType::Int),
                )),
            ])),
        );
        let list_field = NestedField::optional(
            3,
            "tags",
            Type::List(ListType::new(Arc::new(NestedField::list_element(
                20,
                Type::Primitive(PrimitiveType::String),
                false,
            )))),
        );

        let iceberg_schema = Schema::builder()
            .with_schema_id(5)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(struct_field),
                Arc::new(list_field),
            ])
            .build()
            .unwrap();

        let table_schema = iceberg_schema_to_table_schema(&iceberg_schema);

        assert_eq!(table_schema.schema_id, 5);
        assert_eq!(table_schema.fields.len(), 3);

        // Primitive field
        assert_eq!(table_schema.fields[0].name, "id");
        assert_eq!(table_schema.fields[0].field_type, "long");

        // Struct field must be JSON, not Display format
        let telemetry = &table_schema.fields[1];
        assert_eq!(telemetry.name, "telemetry");
        assert!(
            telemetry.field_type.starts_with('{'),
            "struct field must be JSON"
        );
        let telemetry_json: serde_json::Value =
            serde_json::from_str(&telemetry.field_type).unwrap();
        assert_eq!(telemetry_json["type"], "struct");
        let fields = telemetry_json["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0]["name"], "speed");
        assert_eq!(fields[0]["type"], "double");
        assert_eq!(fields[1]["name"], "gear");
        assert_eq!(fields[1]["type"], "int");

        // List field must be JSON
        let tags = &table_schema.fields[2];
        assert_eq!(tags.name, "tags");
        assert!(tags.field_type.starts_with('{'), "list field must be JSON");
        let tags_json: serde_json::Value = serde_json::from_str(&tags.field_type).unwrap();
        assert_eq!(tags_json["type"], "list");
        assert_eq!(tags_json["element"], "string");
    }

    #[test]
    fn schema_roundtrip_with_nested_types() {
        // Full round-trip: TableSchema (with nested JSON types) -> iceberg Schema
        // -> TableSchema. The nested type strings must survive the round-trip.
        let schema_with_nested = TableSchema {
schema_id: 3,
fields: vec![
SchemaFieldInfo {
id: 1,
name: "id".into(),
field_type: "long".into(),
required: true,
doc: None,
},
SchemaFieldInfo {
id: 2,
name: "telemetry".into(),
field_type: r#"{"type":"struct","fields":[{"id":10,"name":"speed","required":false,"type":"double"},{"id":11,"name":"gear","required":false,"type":"int"}]}"#.into(),
required: false,
doc: None,
},
],
        };

        let iceberg_schema = table_schema_to_iceberg_schema(&schema_with_nested).unwrap();
        let back = iceberg_schema_to_table_schema(&iceberg_schema);

        assert_eq!(back.schema_id, schema_with_nested.schema_id);
        assert_eq!(back.fields.len(), 2);
        assert_eq!(back.fields[0].field_type, "long");

        // The nested struct JSON must round-trip
        let telemetry: serde_json::Value =
            serde_json::from_str(&back.fields[1].field_type).unwrap();
        assert_eq!(telemetry["type"], "struct");
        let fields = telemetry["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0]["name"], "speed");
        assert_eq!(fields[0]["type"], "double");
    }
}
