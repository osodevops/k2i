//! Local development command.

use crate::commands::ingest;
use anyhow::{Context, Result};
use k2i_core::config::{
    BufferConfig, CatalogManagerConfig, CatalogType, IcebergConfig, KafkaConfig, KafkaFormatConfig,
    KafkaSecurityConfig, MaintenanceConfig, MonitoringConfig, ObjectStoreConfig, ObjectStoreType,
    OffsetReset, ParquetCompression, ProtobufFormatConfig, RpcConfig, SchemaEvolutionRuntimeConfig,
    SqlCatalogBackend, SqlCatalogConfig, TableManagementConfig, TransactionLogConfig,
};
use k2i_core::Config;
use std::path::PathBuf;

/// Options for running a zero-cloud local K2I instance.
pub struct DevOptions {
    pub topic: String,
    pub warehouse: PathBuf,
    pub bootstrap_servers: String,
    pub database: String,
    pub table: String,
    pub consumer_group: String,
    pub schema_registry_url: Option<String>,
    pub message_type: Option<String>,
}

/// Run a zero-cloud local K2I instance.
pub async fn run(options: DevOptions) -> Result<()> {
    let DevOptions {
        topic,
        warehouse,
        bootstrap_servers,
        database,
        table,
        consumer_group,
        schema_registry_url,
        message_type,
    } = options;

    std::fs::create_dir_all(&warehouse)
        .with_context(|| format!("failed to create warehouse {}", warehouse.display()))?;
    let txlog_dir = warehouse.join("txlog");
    let run_dir = warehouse.join("run");
    std::fs::create_dir_all(&txlog_dir)?;
    std::fs::create_dir_all(&run_dir)?;

    let config = Config {
        kafka: KafkaConfig {
            bootstrap_servers: bootstrap_servers.split(',').map(String::from).collect(),
            topic,
            consumer_group,
            batch_size: 1000,
            batch_timeout_ms: 500,
            session_timeout_ms: 30000,
            heartbeat_interval_ms: 3000,
            max_poll_interval_ms: 300000,
            auto_offset_reset: OffsetReset::Earliest,
            security: KafkaSecurityConfig::default(),
            format: match schema_registry_url {
                Some(schema_registry_url) => KafkaFormatConfig::Protobuf(ProtobufFormatConfig {
                    schema_registry_url,
                    subject_strategy: Default::default(),
                    message_type,
                    cache_ttl_seconds: 300,
                    latest_on_startup: true,
                }),
                None => KafkaFormatConfig::Raw,
            },
        },
        schema_evolution: SchemaEvolutionRuntimeConfig::default(),
        iceberg: IcebergConfig {
            catalog_type: CatalogType::Sql,
            warehouse_path: warehouse.to_string_lossy().to_string(),
            database_name: database,
            table_name: table,
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
                url: format!("sqlite://{}", warehouse.join("catalog.db").display()),
                catalog_name: "k2i_local".to_string(),
            }),
            object_store: ObjectStoreConfig {
                r#type: ObjectStoreType::Filesystem,
                root: Some(warehouse.to_string_lossy().to_string()),
            },
        },
        buffer: BufferConfig::default(),
        transaction_log: TransactionLogConfig {
            log_dir: txlog_dir,
            ..TransactionLogConfig::default()
        },
        maintenance: MaintenanceConfig {
            compaction_enabled: false,
            snapshot_expiration_enabled: false,
            orphan_cleanup_enabled: false,
            statistics_enabled: false,
            ..MaintenanceConfig::default()
        },
        monitoring: MonitoringConfig::default(),
        rpc: RpcConfig {
            enabled: true,
            socket_path: run_dir.join("k2i.sock"),
            ..RpcConfig::default()
        },
    };

    ingest::run(config, None, None, None).await
}
