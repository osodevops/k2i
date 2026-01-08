//! Ingest command implementation.

use crate::server::{start_server, ServerState};
use anyhow::Result;
use k2i_core::engine::IngestionEngine;
use k2i_core::Config;
use std::sync::Arc;
use tracing::info;

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

    let mut engine = IngestionEngine::new(config).await?;
    let shutdown_tx = engine.shutdown_signal();

    // Get references to health and metrics for the HTTP server
    let health = Arc::new(k2i_core::health::HealthCheck::new());
    health.register_component("kafka");
    health.register_component("buffer");
    health.register_component("iceberg");
    health.register_component("txlog");
    health.mark_healthy("txlog"); // Transaction log is healthy if we got this far

    let metrics = Arc::new(k2i_core::metrics::IngestionMetrics::new());

    let server_state = Arc::new(ServerState {
        health: Arc::clone(&health),
        metrics: Arc::clone(&metrics),
    });

    // Start HTTP servers
    let server_shutdown_rx = shutdown_tx.subscribe();
    tokio::spawn(start_server(
        server_state,
        health_port,
        metrics_port,
        server_shutdown_rx,
    ));

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
