use anyhow::Result;
use k2i_core::buffer::HotBuffer;
use k2i_core::config::{
    BufferConfig, CatalogManagerConfig, CatalogType, Config, GlueCatalogConfig, IcebergConfig,
    KafkaConfig, KafkaFormatConfig, KafkaSecurityConfig, MaintenanceConfig, MonitoringConfig,
    ObjectStoreConfig, OffsetReset, ParquetCompression, RestCatalogConfig, RpcConfig,
    SchemaEvolutionRuntimeConfig, TableManagementConfig, TransactionLogConfig,
};
use k2i_core::kafka::KafkaMessage;
use k2i_core::read::{RegisterDataFile, TableReadRegistry};
use k2i_rpc_server::start_rpc_server;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<()> {
    let socket_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/k2i-fixture/k2i.sock"));

    let config = fixture_config(socket_path);
    let hot_buffer = Arc::new(HotBuffer::new(config.buffer.clone()));
    let registry = Arc::new(TableReadRegistry::new(&config, hot_buffer.clone()));
    seed_fixture_data(&registry, &hot_buffer)?;

    let rpc_config = config.rpc.clone();
    let socket_path = rpc_config.socket_path.clone();
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let server = tokio::spawn(start_rpc_server(registry, rpc_config, shutdown_rx));

    println!("{}", socket_path.display());
    tokio::signal::ctrl_c().await?;
    let _ = shutdown_tx.send(());
    server.await??;

    Ok(())
}

fn seed_fixture_data(registry: &Arc<TableReadRegistry>, hot_buffer: &Arc<HotBuffer>) -> Result<()> {
    registry.register_data_file(RegisterDataFile {
        file_path: "data/raw/events/part-00001.parquet".to_string(),
        size_bytes: 1024,
        row_count: 2,
        topic: "events".to_string(),
        partition: 0,
        min_offset: 10,
        max_offset: 11,
        min_lsn: 1,
        max_lsn: 2,
        snapshot_id: 1001,
    })?;

    for offset in 12..15 {
        let lsn = registry.next_lsn();
        hot_buffer.append_with_lsn(&message(offset), lsn)?;
    }

    Ok(())
}

fn message(offset: i64) -> KafkaMessage {
    KafkaMessage {
        key: Some(format!("key-{}", offset).into_bytes()),
        value: Some(format!("value-{}", offset).into_bytes()),
        topic: "events".to_string(),
        partition: 0,
        offset,
        timestamp: 1_700_000_000_000 + offset,
        headers: vec![],
    }
}

fn fixture_config(socket_path: PathBuf) -> Config {
    Config {
        kafka: KafkaConfig {
            bootstrap_servers: vec!["localhost:9092".into()],
            topic: "events".into(),
            consumer_group: "k2i-fixture".into(),
            batch_size: 10,
            batch_timeout_ms: 100,
            session_timeout_ms: 30_000,
            heartbeat_interval_ms: 3_000,
            max_poll_interval_ms: 300_000,
            auto_offset_reset: OffsetReset::Earliest,
            security: KafkaSecurityConfig::default(),
            format: KafkaFormatConfig::Raw,
        },
        schema_evolution: SchemaEvolutionRuntimeConfig::default(),
        iceberg: IcebergConfig {
            catalog_type: CatalogType::Rest,
            warehouse_path: "/tmp/k2i-fixture/warehouse".into(),
            database_name: "raw".into(),
            table_name: "events".into(),
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
            rest: RestCatalogConfig::default(),
            glue: GlueCatalogConfig::default(),
            nessie: None,
            sql_catalog: None,
            object_store: ObjectStoreConfig::default(),
        },
        buffer: BufferConfig::default(),
        transaction_log: TransactionLogConfig::default(),
        maintenance: MaintenanceConfig::default(),
        monitoring: MonitoringConfig::default(),
        rpc: RpcConfig {
            enabled: true,
            socket_path,
            ..RpcConfig::default()
        },
    }
}
