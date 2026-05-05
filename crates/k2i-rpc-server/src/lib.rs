//! Unix socket server for the K2I read-state RPC protocol.

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use k2i_core::config::RpcConfig;
use k2i_core::read::TableReadRegistry;
use k2i_rpc::{decode_request, encode_response, RpcErrorBody, RpcRequest, RpcResponse};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::{sleep, Instant};
use tracing::{debug, error, info};

#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};

#[cfg(unix)]
use tokio_util::codec::LengthDelimitedCodec;

/// Start the read-state RPC server.
#[cfg(unix)]
pub async fn start_rpc_server(
    registry: Arc<TableReadRegistry>,
    config: RpcConfig,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }

    prepare_socket_path(&config.socket_path)?;
    let listener = UnixListener::bind(&config.socket_path).with_context(|| {
        format!(
            "failed to bind K2I RPC socket at {}",
            config.socket_path.display()
        )
    })?;

    info!(path = %config.socket_path.display(), "K2I read RPC server started");

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("K2I read RPC server shutdown requested");
                break;
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _addr)) => {
                        let registry = registry.clone();
                        let config = config.clone();
                        tokio::spawn(async move {
                            if let Err(error) = handle_connection(stream, registry, config).await {
                                debug!(%error, "K2I read RPC connection closed with error");
                            }
                        });
                    }
                    Err(error) => {
                        error!(%error, "Failed to accept K2I read RPC connection");
                    }
                }
            }
        }
    }

    cleanup_socket_path(&config.socket_path);
    Ok(())
}

/// Non-Unix platforms do not support the v1 local socket transport.
#[cfg(not(unix))]
pub async fn start_rpc_server(
    _registry: Arc<TableReadRegistry>,
    _config: RpcConfig,
    _shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    anyhow::bail!("K2I read RPC server requires Unix domain sockets")
}

#[cfg(unix)]
async fn handle_connection(
    stream: UnixStream,
    registry: Arc<TableReadRegistry>,
    config: RpcConfig,
) -> Result<()> {
    let mut framed = LengthDelimitedCodec::builder()
        .length_field_type::<u32>()
        .little_endian()
        .max_frame_length(config.max_frame_bytes)
        .new_framed(stream);

    while let Some(frame) = framed.next().await {
        let frame = frame.context("failed to read K2I RPC frame")?;
        let response = match decode_request(&frame, config.max_frame_bytes) {
            Ok(request) => dispatch_request(registry.clone(), request, &config).await,
            Err(error) => RpcResponse::Error(RpcErrorBody::new(
                "decode_error",
                format!("failed to decode request: {}", error),
            )),
        };

        let response = encode_response(&response).context("failed to encode K2I RPC response")?;
        framed
            .send(Bytes::from(response))
            .await
            .context("failed to write K2I RPC response")?;
    }

    Ok(())
}

async fn dispatch_request(
    registry: Arc<TableReadRegistry>,
    request: RpcRequest,
    config: &RpcConfig,
) -> RpcResponse {
    match request {
        RpcRequest::Health => RpcResponse::Health {
            ok: true,
            current_lsn: registry.current_lsn(),
            message: None,
        },
        RpcRequest::ListTables => RpcResponse::Tables(registry.list_tables()),
        RpcRequest::GetTableSchema { database, table } => {
            match registry.table_schema_ipc(&database, &table) {
                Ok(schema) => RpcResponse::TableSchema(schema),
                Err(error) => rpc_error("schema_error", error),
            }
        }
        RpcRequest::ScanTableBegin {
            database,
            table,
            lsn_bound,
        } => {
            if let Err(error) =
                wait_for_lsn(registry.clone(), lsn_bound, config.read_timeout_ms).await
            {
                return rpc_error("lsn_timeout", error);
            }

            match registry.begin_scan(&database, &table, lsn_bound) {
                Ok(state) => RpcResponse::ReadState(state),
                Err(error) => rpc_error("scan_begin_error", error),
            }
        }
        RpcRequest::ScanTableEnd {
            database,
            table,
            scan_id,
        } => match registry.end_scan(&database, &table, scan_id) {
            Ok(()) => RpcResponse::Ack,
            Err(error) => rpc_error("scan_end_error", error),
        },
    }
}

async fn wait_for_lsn(
    registry: Arc<TableReadRegistry>,
    lsn_bound: Option<u64>,
    read_timeout_ms: u64,
) -> Result<()> {
    let Some(bound) = lsn_bound else {
        return Ok(());
    };

    let deadline = Instant::now() + Duration::from_millis(read_timeout_ms);
    while registry.current_lsn() < bound {
        if Instant::now() >= deadline {
            anyhow::bail!(
                "requested LSN {} is ahead of current LSN {}",
                bound,
                registry.current_lsn()
            );
        }

        sleep(Duration::from_millis(5)).await;
    }

    Ok(())
}

fn rpc_error(code: &str, error: impl std::fmt::Display) -> RpcResponse {
    RpcResponse::Error(RpcErrorBody::new(code, error.to_string()))
}

#[cfg(unix)]
fn prepare_socket_path(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create K2I RPC socket directory {}",
                parent.display()
            )
        })?;
    }

    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to remove stale K2I RPC socket {}", path.display())
            });
        }
    }

    Ok(())
}

#[cfg(unix)]
fn cleanup_socket_path(path: &Path) {
    if let Err(error) = std::fs::remove_file(path) {
        if error.kind() != std::io::ErrorKind::NotFound {
            debug!(
                path = %path.display(),
                %error,
                "Failed to remove K2I RPC socket during shutdown"
            );
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use futures::{SinkExt, StreamExt};
    use k2i_core::buffer::HotBuffer;
    use k2i_core::config::{
        BufferConfig, CatalogManagerConfig, CatalogType, Config, GlueCatalogConfig, IcebergConfig,
        KafkaConfig, KafkaFormatConfig, KafkaSecurityConfig, MaintenanceConfig, MonitoringConfig,
        ObjectStoreConfig, OffsetReset, ParquetCompression, RestCatalogConfig, RpcConfig,
        SchemaEvolutionRuntimeConfig, TableManagementConfig, TransactionLogConfig,
    };
    use k2i_rpc::{decode_response, encode_request};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::net::UnixStream;
    use tokio::sync::broadcast;
    use tokio_util::codec::LengthDelimitedCodec;

    fn test_config(temp_dir: &TempDir) -> Config {
        Config {
            kafka: KafkaConfig {
                bootstrap_servers: vec!["localhost:9092".into()],
                topic: "events".into(),
                consumer_group: "k2i".into(),
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
                warehouse_path: temp_dir
                    .path()
                    .join("warehouse")
                    .to_string_lossy()
                    .to_string(),
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
            rpc: RpcConfig::default(),
        }
    }

    async fn wait_for_socket(path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !path.exists() {
            assert!(Instant::now() < deadline, "RPC socket was not created");
            sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn socket_round_trip_health_and_list_tables() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = test_config(&temp_dir);
        config.rpc.enabled = true;
        config.rpc.socket_path = temp_dir.path().join("k2i.sock");

        let hot_buffer = Arc::new(HotBuffer::new(config.buffer.clone()));
        let registry = Arc::new(TableReadRegistry::new(&config, hot_buffer));
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let server_config = config.rpc.clone();
        let server_registry = registry.clone();
        let server = tokio::spawn(async move {
            start_rpc_server(server_registry, server_config, shutdown_rx)
                .await
                .unwrap();
        });

        wait_for_socket(&config.rpc.socket_path).await;

        let stream = UnixStream::connect(&config.rpc.socket_path).await.unwrap();
        let mut framed = LengthDelimitedCodec::builder()
            .length_field_type::<u32>()
            .little_endian()
            .max_frame_length(config.rpc.max_frame_bytes)
            .new_framed(stream);

        let health = encode_request(&RpcRequest::Health).unwrap();
        framed.send(Bytes::from(health)).await.unwrap();
        let response = framed.next().await.unwrap().unwrap();
        let response = decode_response(&response, config.rpc.max_frame_bytes).unwrap();
        assert_eq!(
            response,
            RpcResponse::Health {
                ok: true,
                current_lsn: 0,
                message: None
            }
        );

        let list_tables = encode_request(&RpcRequest::ListTables).unwrap();
        framed.send(Bytes::from(list_tables)).await.unwrap();
        let response = framed.next().await.unwrap().unwrap();
        let response = decode_response(&response, config.rpc.max_frame_bytes).unwrap();
        match response {
            RpcResponse::Tables(tables) => {
                assert_eq!(tables.len(), 1);
                assert_eq!(tables[0].database, "raw");
                assert_eq!(tables[0].table, "events");
            }
            other => panic!("unexpected response: {:?}", other),
        }

        let _ = shutdown_tx.send(());
        server.await.unwrap();
    }
}
