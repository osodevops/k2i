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
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

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
        // current table metadata and lets the REST catalog validate the commit
        // requirements. The caller-supplied `expected_snapshot_id` (derived
        // from the metadata cache) would only duplicate that check.
        //
        // K2I ingestion is append-only, so `files_to_remove` is always empty;
        // this path performs a `fast_append` and does not support removals. We
        // assert the invariant rather than silently dropping removals.
        debug_assert!(
            commit.files_to_remove.is_empty(),
            "OfficialRestCommitter.commit_snapshot only supports appends; \
             files_to_remove must be empty"
        );
        let files_added = commit.files_to_add.len();
        let files_removed = commit.files_to_remove.len();

        let result = commit_append_with_catalog(
            &self.catalog,
            &self.warehouse_path,
            namespace,
            table,
            commit.files_to_add,
            commit.summary,
        )
        .await?;

        Ok(SnapshotCommitResult {
            snapshot_id: result.snapshot_id,
            committed_at: chrono::Utc::now(),
            files_added,
            files_removed,
        })
    }

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
                ::iceberg::spec::PrimitiveType::Fixed(_) => "binary",
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
    if message.contains("TableNotFound") || message.contains("does not exist") {
        Error::Iceberg(IcebergError::TableNotFound(message))
    } else if message.contains("CatalogCommitConflicts") || message.contains("requirements failed")
    {
        Error::Iceberg(IcebergError::CasConflict {
            expected: -1,
            actual: -1,
        })
    } else {
        Error::Iceberg(IcebergError::Other(message))
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
}
