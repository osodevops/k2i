//! Register existing Parquet files as K2I-readable table data.

use crate::config::{CatalogType, Config};
use crate::format::protobuf::ProtobufSchemaResolver;
use crate::iceberg::{
    CatalogFactoryRegistry, DataFileInfo, OfficialRestCommitter, SnapshotCommit, TableSchema,
};
use crate::txlog::{RecoveryState, TransactionEntry, TransactionLog};
use crate::{Error, IcebergError, Result};
use arrow::datatypes::Schema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Source of the table schema for backfill registration.
#[derive(Debug, Clone)]
pub enum BackfillSchemaSource {
    /// Infer table schema from the first registered Parquet file.
    FirstFile,
    /// Use an explicit table schema.
    TableSchema(TableSchema),
}

/// Options for registering existing files.
#[derive(Debug, Clone)]
pub struct BackfillRegisterOptions {
    /// Database/namespace to register.
    pub database: String,
    /// Table name to register.
    pub table: String,
    /// Parquet files to register.
    pub source_files: Vec<PathBuf>,
    /// Schema source.
    pub schema_source: BackfillSchemaSource,
    /// Optional topic label for read-state provenance.
    pub kafka_topic: Option<String>,
}

/// Summary of registered files.
#[derive(Debug, Clone)]
pub struct BackfillRegisterReport {
    /// Database/namespace.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Snapshot committed by the catalog.
    pub snapshot_id: i64,
    /// Number of files registered.
    pub files_registered: usize,
    /// Number of rows registered.
    pub rows_registered: u64,
}

/// Options for resetting a local table registration.
#[derive(Debug, Clone)]
pub struct BackfillResetOptions {
    /// Database/namespace.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Keep existing Parquet files on disk/object storage.
    pub keep_data: bool,
}

#[derive(Debug, Clone)]
struct ParquetFileInfo {
    path: PathBuf,
    schema: Arc<Schema>,
    size_bytes: u64,
    row_count: u64,
}

/// Register existing Parquet files in the configured catalog and read-state log.
pub async fn register_parquet_files(
    config: &Config,
    options: BackfillRegisterOptions,
) -> Result<BackfillRegisterReport> {
    if options.source_files.is_empty() {
        return Err(Error::Config(
            "at least one --source Parquet file is required".into(),
        ));
    }

    let mut files = Vec::with_capacity(options.source_files.len());
    for source in &options.source_files {
        files.push(inspect_parquet_file(source)?);
    }
    validate_compatible_schemas(&files)?;

    let table_schema = match options.schema_source {
        BackfillSchemaSource::FirstFile => {
            ProtobufSchemaResolver::table_schema_from_arrow(1, files[0].schema.as_ref())
        }
        BackfillSchemaSource::TableSchema(schema) => schema,
    };

    let catalog = CatalogFactoryRegistry::with_defaults()
        .create(&config.iceberg)
        .await?;

    if catalog
        .table_exists(&options.database, &options.table)
        .await?
    {
        let existing = catalog
            .load_table(&options.database, &options.table)
            .await?;
        if existing.schema != table_schema {
            return Err(Error::Iceberg(IcebergError::SchemaMismatch {
                expected: format!("{:?}", existing.schema),
                actual: format!("{:?}", table_schema),
            }));
        }
    } else {
        catalog
            .create_table(&options.database, &options.table, &table_schema)
            .await?;
    }

    let expected_snapshot_id = catalog
        .current_snapshot_id(&options.database, &options.table)
        .await?;
    let files_to_add = files
        .iter()
        .map(|file| DataFileInfo {
            file_path: file.path.to_string_lossy().to_string(),
            file_size_bytes: file.size_bytes,
            record_count: file.row_count,
            partition_values: HashMap::new(),
            file_format: "parquet".to_string(),
        })
        .collect::<Vec<_>>();

    let mut summary = HashMap::new();
    summary.insert("operation".to_string(), "backfill-register".to_string());
    summary.insert(
        "added-data-files".to_string(),
        files_to_add.len().to_string(),
    );
    summary.insert(
        "added-records".to_string(),
        files
            .iter()
            .map(|file| file.row_count)
            .sum::<u64>()
            .to_string(),
    );

    let snapshot_id = if OfficialRestCommitter::is_enabled(&config.iceberg) {
        let committer = OfficialRestCommitter::new(&config.iceberg).await?;
        committer
            .commit_append(
                &options.database,
                &options.table,
                files_to_add.clone(),
                summary.clone(),
            )
            .await?
            .snapshot_id
    } else {
        let commit = SnapshotCommit {
            expected_snapshot_id,
            files_to_add: files_to_add.clone(),
            files_to_remove: vec![],
            summary,
        };
        catalog
            .commit_snapshot(&options.database, &options.table, commit)
            .await?
            .snapshot_id
    };

    let txlog = TransactionLog::open(config.transaction_log.clone())?;
    let recovery = RecoveryState::recover_from(&txlog)?;
    let mut next_lsn = recovery
        .read_data_files
        .iter()
        .filter(|file| file.database == options.database && file.table == options.table)
        .map(|file| file.read_lsn)
        .max()
        .unwrap_or(0);
    let topic = options
        .kafka_topic
        .unwrap_or_else(|| "__backfill__".to_string());
    let mut rows_registered = 0_u64;

    for (index, file) in files.iter().enumerate() {
        let min_lsn = next_lsn + 1;
        next_lsn += file.row_count.max(1);
        rows_registered += file.row_count;
        txlog.append(TransactionEntry::DataFileCommitted {
            min_lsn,
            read_lsn: next_lsn,
            database: options.database.clone(),
            table: options.table.clone(),
            topic: topic.clone(),
            partition: 0,
            min_offset: index as i64,
            max_offset: index as i64,
            snapshot_id,
            file_path: file.path.to_string_lossy().to_string(),
            file_size_bytes: file.size_bytes,
            row_count: file.row_count,
            timestamp: chrono::Utc::now(),
        })?;
    }

    Ok(BackfillRegisterReport {
        database: options.database,
        table: options.table,
        snapshot_id,
        files_registered: files.len(),
        rows_registered,
    })
}

/// Reset a local table registration while optionally retaining data files.
pub async fn reset_table_registration(
    config: &Config,
    options: BackfillResetOptions,
) -> Result<()> {
    if !options.keep_data {
        return Err(Error::Config(
            "table reset currently requires --keep-data to avoid deleting source files".into(),
        ));
    }

    if config.iceberg.catalog_type == CatalogType::Sql {
        let sql_catalog = crate::iceberg::SqlCatalogClient::connect(&config.iceberg).await?;
        sql_catalog
            .reset_table(&options.database, &options.table)
            .await?;
    }

    let txlog = TransactionLog::open(config.transaction_log.clone())?;
    txlog.append(TransactionEntry::TableReset {
        database: options.database,
        table: options.table,
        keep_data: options.keep_data,
        timestamp: chrono::Utc::now(),
    })?;
    Ok(())
}

fn inspect_parquet_file(path: &Path) -> Result<ParquetFileInfo> {
    let canonical = path.canonicalize().map_err(|e| {
        Error::Storage(format!(
            "failed to resolve Parquet source {}: {}",
            path.display(),
            e
        ))
    })?;
    let metadata = std::fs::metadata(&canonical)?;
    if !metadata.is_file() {
        return Err(Error::Storage(format!(
            "Parquet source {} is not a file",
            canonical.display()
        )));
    }

    let file = File::open(&canonical)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        Error::Storage(format!(
            "failed to read Parquet metadata for {}: {}",
            canonical.display(),
            e
        ))
    })?;
    let row_count = builder.metadata().file_metadata().num_rows().max(0) as u64;

    Ok(ParquetFileInfo {
        path: canonical,
        schema: builder.schema().clone(),
        size_bytes: metadata.len(),
        row_count,
    })
}

fn validate_compatible_schemas(files: &[ParquetFileInfo]) -> Result<()> {
    if files.is_empty() {
        return Ok(());
    }
    let expected = files[0].schema.as_ref();
    let mismatches = files
        .iter()
        .skip(1)
        .filter(|file| !schemas_compatible(expected, file.schema.as_ref()))
        .map(|file| file.path.display().to_string())
        .collect::<Vec<_>>();

    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(Error::Iceberg(IcebergError::SchemaMismatch {
            expected: format!("{:?}", expected.fields()),
            actual: format!("mismatched files: {}", mismatches.join(", ")),
        }))
    }
}

fn schemas_compatible(left: &Schema, right: &Schema) -> bool {
    if left.fields().len() != right.fields().len() {
        return false;
    }

    left.fields().iter().zip(right.fields()).all(|(a, b)| {
        a.name() == b.name() && a.data_type() == b.data_type() && a.is_nullable() == b.is_nullable()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        BufferConfig, CatalogManagerConfig, CatalogType, IcebergConfig, KafkaConfig,
        KafkaFormatConfig, KafkaSecurityConfig, MaintenanceConfig, MonitoringConfig,
        ObjectStoreConfig, OffsetReset, ParquetCompression, RpcConfig,
        SchemaEvolutionRuntimeConfig, SqlCatalogBackend, SqlCatalogConfig, TableManagementConfig,
        TransactionLogConfig,
    };
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_config(temp: &TempDir) -> Config {
        Config {
            kafka: KafkaConfig {
                bootstrap_servers: vec!["localhost:9092".to_string()],
                topic: "test".to_string(),
                consumer_group: "test".to_string(),
                batch_size: 100,
                batch_timeout_ms: 1000,
                session_timeout_ms: 30000,
                heartbeat_interval_ms: 3000,
                max_poll_interval_ms: 300000,
                auto_offset_reset: OffsetReset::Earliest,
                security: KafkaSecurityConfig::default(),
                format: KafkaFormatConfig::Raw,
            },
            schema_evolution: SchemaEvolutionRuntimeConfig::default(),
            iceberg: IcebergConfig {
                catalog_type: CatalogType::Sql,
                warehouse_path: temp.path().join("warehouse").to_string_lossy().to_string(),
                database_name: "f1".to_string(),
                table_name: "historical".to_string(),
                target_file_size_mb: 512,
                compression: ParquetCompression::Snappy,
                partition_spec: vec![],
                rest_uri: None,
                hive_metastore_uri: None,
                aws_region: None,
                aws_access_key_id: None,
                aws_secret_access_key: None,
                s3_endpoint: None,
                catalog_manager: CatalogManagerConfig::default(),
                table_management: TableManagementConfig::default(),
                rest: Default::default(),
                glue: Default::default(),
                nessie: None,
                sql_catalog: Some(SqlCatalogConfig {
                    r#type: SqlCatalogBackend::Sqlite,
                    url: format!("sqlite://{}", temp.path().join("catalog.db").display()),
                    catalog_name: "k2i_test".to_string(),
                }),
                object_store: ObjectStoreConfig::default(),
            },
            buffer: BufferConfig::default(),
            transaction_log: TransactionLogConfig {
                log_dir: temp.path().join("txlog"),
                ..TransactionLogConfig::default()
            },
            maintenance: MaintenanceConfig::default(),
            monitoring: MonitoringConfig::default(),
            rpc: RpcConfig::default(),
        }
    }

    fn write_parquet(path: &Path, rows: i32) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("driver", DataType::Utf8, false),
            Field::new("lap", DataType::Int32, false),
        ]));
        let drivers = (0..rows)
            .map(|idx| format!("driver-{idx}"))
            .collect::<Vec<_>>();
        let laps = (0..rows).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(drivers)),
                Arc::new(Int32Array::from(laps)),
            ],
        )
        .unwrap();
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn register_parquet_files_commits_catalog_and_txlog() {
        let temp = TempDir::new().unwrap();
        let config = test_config(&temp);
        let parquet_path = temp.path().join("historical.parquet");
        write_parquet(&parquet_path, 3);

        let report = register_parquet_files(
            &config,
            BackfillRegisterOptions {
                database: "f1".to_string(),
                table: "historical".to_string(),
                source_files: vec![parquet_path.clone()],
                schema_source: BackfillSchemaSource::FirstFile,
                kafka_topic: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(report.files_registered, 1);
        assert_eq!(report.rows_registered, 3);

        let txlog = TransactionLog::open(config.transaction_log.clone()).unwrap();
        let recovery = RecoveryState::recover_from(&txlog).unwrap();
        assert_eq!(recovery.read_data_files.len(), 1);
        assert_eq!(recovery.read_data_files[0].row_count, 3);
        assert_eq!(
            recovery.read_data_files[0].file_path,
            parquet_path.canonicalize().unwrap().display().to_string()
        );
    }

    #[tokio::test]
    async fn reset_removes_recovered_read_files() {
        let temp = TempDir::new().unwrap();
        let config = test_config(&temp);
        let parquet_path = temp.path().join("historical.parquet");
        write_parquet(&parquet_path, 2);

        register_parquet_files(
            &config,
            BackfillRegisterOptions {
                database: "f1".to_string(),
                table: "historical".to_string(),
                source_files: vec![parquet_path],
                schema_source: BackfillSchemaSource::FirstFile,
                kafka_topic: None,
            },
        )
        .await
        .unwrap();
        reset_table_registration(
            &config,
            BackfillResetOptions {
                database: "f1".to_string(),
                table: "historical".to_string(),
                keep_data: true,
            },
        )
        .await
        .unwrap();

        let txlog = TransactionLog::open(config.transaction_log.clone()).unwrap();
        let recovery = RecoveryState::recover_from(&txlog).unwrap();
        assert!(recovery.read_data_files.is_empty());
    }
}
