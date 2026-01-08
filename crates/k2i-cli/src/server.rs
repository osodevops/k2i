//! HTTP server for health and metrics endpoints.

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use k2i_core::health::{ComponentStatus, HealthCheck, HealthStatus};
use k2i_core::metrics::IngestionMetrics;
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{error, info};

/// Shared state for HTTP endpoints.
pub struct ServerState {
    pub health: Arc<HealthCheck>,
    pub metrics: Arc<IngestionMetrics>,
}

/// Health response structure.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub components: std::collections::HashMap<String, ComponentStatusResponse>,
}

/// Component status response.
#[derive(Debug, Serialize)]
pub struct ComponentStatusResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl From<&ComponentStatus> for ComponentStatusResponse {
    fn from(status: &ComponentStatus) -> Self {
        match status {
            ComponentStatus::Healthy => ComponentStatusResponse {
                status: "healthy".to_string(),
                message: None,
            },
            ComponentStatus::Degraded(msg) => ComponentStatusResponse {
                status: "degraded".to_string(),
                message: Some(msg.clone()),
            },
            ComponentStatus::Unhealthy(msg) => ComponentStatusResponse {
                status: "unhealthy".to_string(),
                message: Some(msg.clone()),
            },
            ComponentStatus::Unknown => ComponentStatusResponse {
                status: "unknown".to_string(),
                message: None,
            },
        }
    }
}

/// Start the HTTP server for health and metrics.
pub async fn start_server(
    state: Arc<ServerState>,
    health_port: u16,
    metrics_port: u16,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    // Start health server
    let health_state = Arc::clone(&state);
    let health_addr = SocketAddr::from(([0, 0, 0, 0], health_port));
    let health_router = Router::new()
        .route("/health", get(health_handler))
        .route("/healthz", get(healthz_handler))
        .route("/readyz", get(readyz_handler))
        .with_state(health_state);

    let health_listener = match tokio::net::TcpListener::bind(health_addr).await {
        Ok(listener) => listener,
        Err(e) => {
            error!(error = %e, port = health_port, "Failed to bind health server");
            return;
        }
    };

    info!(port = health_port, "Health server started");

    // Start metrics server
    let metrics_state = Arc::clone(&state);
    let metrics_addr = SocketAddr::from(([0, 0, 0, 0], metrics_port));
    let metrics_router = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(metrics_state);

    let metrics_listener = match tokio::net::TcpListener::bind(metrics_addr).await {
        Ok(listener) => listener,
        Err(e) => {
            error!(error = %e, port = metrics_port, "Failed to bind metrics server");
            return;
        }
    };

    info!(port = metrics_port, "Metrics server started");

    // Spawn servers with graceful shutdown
    let health_server = async {
        axum::serve(health_listener, health_router)
            .await
            .map_err(|e| error!(error = %e, "Health server error"))
    };

    let metrics_server = async {
        axum::serve(metrics_listener, metrics_router)
            .await
            .map_err(|e| error!(error = %e, "Metrics server error"))
    };

    tokio::select! {
        _ = health_server => {}
        _ = metrics_server => {}
        _ = shutdown_rx.recv() => {
            info!("HTTP servers shutting down");
        }
    }
}

/// Health endpoint handler.
async fn health_handler(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    let overall = state.health.overall_status();
    let statuses = state.health.get_all_statuses();

    let components: std::collections::HashMap<String, ComponentStatusResponse> = statuses
        .iter()
        .map(|(k, v)| (k.clone(), ComponentStatusResponse::from(v)))
        .collect();

    let status_str = match overall {
        HealthStatus::Healthy => "healthy",
        HealthStatus::Degraded => "degraded",
        HealthStatus::Unhealthy => "unhealthy",
    };

    let response = HealthResponse {
        status: status_str.to_string(),
        components,
    };

    let status_code = match overall {
        HealthStatus::Healthy => StatusCode::OK,
        HealthStatus::Degraded => StatusCode::OK,
        HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
    };

    (status_code, Json(response))
}

/// Kubernetes liveness probe handler.
async fn healthz_handler(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    // Liveness: Is the process alive?
    let is_alive = state.health.overall_status() != HealthStatus::Unhealthy;

    if is_alive {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Kubernetes readiness probe handler.
async fn readyz_handler(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    // Readiness: Can we accept traffic?
    if state.health.is_operational() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Prometheus metrics endpoint handler.
async fn metrics_handler(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    let metrics = &state.metrics;
    let histogram = metrics.flush_duration_histogram();

    // Format metrics in Prometheus text format
    let mut output = String::with_capacity(4096);

    // === COUNTERS ===
    output.push_str("# HELP k2i_messages_consumed_total Total messages consumed from Kafka\n");
    output.push_str("# TYPE k2i_messages_consumed_total counter\n");
    output.push_str(&format!(
        "k2i_messages_consumed_total {}\n\n",
        metrics.messages_total()
    ));

    output.push_str("# HELP k2i_errors_total Total errors encountered\n");
    output.push_str("# TYPE k2i_errors_total counter\n");
    output.push_str(&format!("k2i_errors_total {}\n\n", metrics.errors_total()));

    output.push_str("# HELP k2i_flushes_total Total buffer flushes to Iceberg\n");
    output.push_str("# TYPE k2i_flushes_total counter\n");
    output.push_str(&format!(
        "k2i_flushes_total {}\n\n",
        metrics.flushes_total()
    ));

    output.push_str("# HELP k2i_rows_flushed_total Total rows flushed to Iceberg\n");
    output.push_str("# TYPE k2i_rows_flushed_total counter\n");
    output.push_str(&format!(
        "k2i_rows_flushed_total {}\n\n",
        metrics.rows_flushed_total()
    ));

    output.push_str("# HELP k2i_backpressure_events_total Total backpressure events\n");
    output.push_str("# TYPE k2i_backpressure_events_total counter\n");
    output.push_str(&format!(
        "k2i_backpressure_events_total {}\n\n",
        metrics.backpressure_total()
    ));

    output.push_str("# HELP k2i_iceberg_commits_total Total Iceberg snapshot commits\n");
    output.push_str("# TYPE k2i_iceberg_commits_total counter\n");
    output.push_str(&format!(
        "k2i_iceberg_commits_total {}\n\n",
        metrics.iceberg_commits_total()
    ));

    // === GAUGES ===
    output.push_str("# HELP k2i_buffer_size_bytes Current hot buffer size in bytes\n");
    output.push_str("# TYPE k2i_buffer_size_bytes gauge\n");
    output.push_str(&format!(
        "k2i_buffer_size_bytes {}\n\n",
        metrics.buffer_size_bytes()
    ));

    output.push_str("# HELP k2i_buffer_record_count Current record count in hot buffer\n");
    output.push_str("# TYPE k2i_buffer_record_count gauge\n");
    output.push_str(&format!(
        "k2i_buffer_record_count {}\n\n",
        metrics.buffer_record_count()
    ));

    // === HISTOGRAM: flush_duration_seconds ===
    output.push_str("# HELP k2i_flush_duration_seconds Time to flush hot buffer to Iceberg\n");
    output.push_str("# TYPE k2i_flush_duration_seconds histogram\n");

    // Bucket entries (cumulative)
    for (le, count) in &histogram.buckets {
        output.push_str(&format!(
            "k2i_flush_duration_seconds_bucket{{le=\"{}\"}} {}\n",
            le, count
        ));
    }
    output.push_str(&format!(
        "k2i_flush_duration_seconds_bucket{{le=\"+Inf\"}} {}\n",
        histogram.inf_bucket
    ));
    output.push_str(&format!(
        "k2i_flush_duration_seconds_sum {}\n",
        histogram.sum_seconds
    ));
    output.push_str(&format!(
        "k2i_flush_duration_seconds_count {}\n",
        histogram.count
    ));

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_component_status_conversion() {
        let healthy = ComponentStatus::Healthy;
        let response: ComponentStatusResponse = (&healthy).into();
        assert_eq!(response.status, "healthy");
        assert!(response.message.is_none());

        let degraded = ComponentStatus::Degraded("slow".to_string());
        let response: ComponentStatusResponse = (&degraded).into();
        assert_eq!(response.status, "degraded");
        assert_eq!(response.message, Some("slow".to_string()));

        let unhealthy = ComponentStatus::Unhealthy("down".to_string());
        let response: ComponentStatusResponse = (&unhealthy).into();
        assert_eq!(response.status, "unhealthy");
        assert_eq!(response.message, Some("down".to_string()));

        let unknown = ComponentStatus::Unknown;
        let response: ComponentStatusResponse = (&unknown).into();
        assert_eq!(response.status, "unknown");
        assert!(response.message.is_none());
    }
}
