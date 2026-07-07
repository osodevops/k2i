//! Official Iceberg metadata commit support.
//!
//! This module uses Apache iceberg-rust transaction APIs to produce real
//! metadata JSON, manifest lists, and manifest files. The existing
//! `CatalogOperations` implementations remain useful for lifecycle and health
//! checks, but append commits that need DuckDB `iceberg_scan` compatibility
//! should go through this path.

use crate::config::{CatalogType, CredentialType, IcebergConfig};
use crate::iceberg::factory::{DataFileInfo, SchemaFieldInfo, TableSchema};
use crate::{Error, IcebergError, Result};
use ::iceberg::CatalogBuilder;
use iceberg_catalog_rest::{
    RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Result of a real Iceberg append commit.
#[derive(Debug, Clone)]
pub struct OfficialCommitResult {
    /// Committed snapshot ID from the Iceberg table metadata.
    pub snapshot_id: i64,
    /// Real manifest-list path for the committed snapshot.
    pub manifest_list_path: String,
}

/// REST-catalog backed official Iceberg committer.
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
        let rest_uri = config
            .rest_uri
            .as_ref()
            .ok_or_else(|| Error::Config("official REST committer requires rest_uri".into()))?;

        let mut props = HashMap::from([
            (REST_CATALOG_PROP_URI.to_string(), rest_uri.clone()),
            (
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                warehouse_uri(&config.warehouse_path)?,
            ),
        ]);

        apply_rest_auth_props(config, &mut props)?;
        apply_file_io_props(config, &mut props);

        for (key, value) in &config.rest.custom_headers {
            props.insert(format!("header.{}", key), value.clone());
        }

        let catalog = RestCatalogBuilder::default()
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
}
