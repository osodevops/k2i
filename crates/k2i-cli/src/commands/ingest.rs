//! Ingest command implementation.

use crate::server::{start_server, ServerState};
use anyhow::Result;
use k2i_core::engine::IngestionEngine;
use k2i_core::Config;
use tracing::{error, info};

#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};

/// Run the ingestion engine.
pub async fn run(
    mut config: Config,
    bootstrap_servers: Option<String>,
    topic: Option<String>,
    consumer_group: Option<String>,
) -> Result<()> {
    // Apply CLI overrides
    if let Some(servers) = bootstrap_servers {
        config.kafka.bootstrap_servers = servers.split(',').map(String::from).collect();
    }
    if let Some(t) = topic {
        config.kafka.topic = t;
    }
    if let Some(g) = consumer_group {
        config.kafka.consumer_group = g;
    }

    info!(
        topic = %config.kafka.topic,
        group = %config.kafka.consumer_group,
        "Starting ingestion engine"
    );

    let health_port = config.monitoring.health_port;
    let metrics_port = config.monitoring.metrics_port;
    let rpc_config = config.rpc.clone();

    let mut engine = IngestionEngine::new(config).await?;
    let shutdown_tx = engine.shutdown_signal();

    let server_state = std::sync::Arc::new(ServerState {
        health: engine.health(),
        metrics: engine.metrics(),
    });

    // Start HTTP servers
    let server_shutdown_rx = shutdown_tx.subscribe();
    tokio::spawn(start_server(
        server_state,
        health_port,
        metrics_port,
        server_shutdown_rx,
    ));

    if rpc_config.enabled {
        let registry = engine.read_registry();
        let rpc_shutdown_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            if let Err(err) =
                k2i_rpc_server::start_rpc_server(registry, rpc_config, rpc_shutdown_rx).await
            {
                error!(error = %err, "K2I read RPC server stopped with error");
            }
        });
    }

    // Spawn a task to handle shutdown signals (SIGINT and SIGTERM)
    let shutdown_signal = shutdown_tx.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut sigterm =
                signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");

            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Received SIGINT (Ctrl+C), initiating graceful shutdown");
                }
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, initiating graceful shutdown");
                }
            }
        }

        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
            info!("Received Ctrl+C, initiating graceful shutdown");
        }

        let _ = shutdown_signal.send(());
    });

    // Run the engine
    engine.run().await?;

    info!("Ingestion engine stopped");
    Ok(())
}
