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

/// Build the router serving health and probe endpoints.
fn health_router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/healthz", get(healthz_handler))
        .route("/startupz", get(startupz_handler))
        .route("/readyz", get(readyz_handler))
        .with_state(state)
}

/// Start the HTTP server for health and metrics.
pub async fn start_server(
    state: Arc<ServerState>,
    health_port: u16,
    metrics_port: u16,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    // Start health server
    let health_addr = SocketAddr::from(([0, 0, 0, 0], health_port));
    let health_router = health_router(Arc::clone(&state));

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
///
/// Liveness only answers "is the process alive and serving HTTP?". It must not
/// depend on component health: a transient Kafka, catalog, or Schema Registry
/// failure during initialization would otherwise get the pod restarted in a
/// loop. Component-aware status stays on `/health` and `/readyz`.
async fn healthz_handler(State(_state): State<Arc<ServerState>>) -> impl IntoResponse {
    StatusCode::OK
}

/// Kubernetes startup probe handler.
///
/// Returns 503 until the ingestion engine has entered its run loop, then 200.
/// Kubernetes disables the liveness and readiness probes until this one
/// succeeds, so slow initialization cannot trip the liveness probe.
async fn startupz_handler(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    if state.health.is_job_running() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Kubernetes readiness probe handler.
async fn readyz_handler(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    // Readiness: Can we accept traffic?
    if state.health.is_ready() {
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

    fn test_state() -> Arc<ServerState> {
        Arc::new(ServerState {
            health: Arc::new(HealthCheck::new()),
            metrics: Arc::new(IngestionMetrics::new()),
        })
    }

    #[tokio::test]
    async fn probe_routes_report_state_over_http() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        async fn get(app: &Router, uri: &str) -> StatusCode {
            app.clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status()
        }

        let state = test_state();
        let app = health_router(Arc::clone(&state));

        assert_eq!(get(&app, "/healthz").await, StatusCode::OK);
        assert_eq!(
            get(&app, "/startupz").await,
            StatusCode::SERVICE_UNAVAILABLE
        );

        state.health.job_started();
        state.health.register_component("kafka");
        state.health.mark_unhealthy("kafka", "connection lost");

        assert_eq!(get(&app, "/healthz").await, StatusCode::OK);
        assert_eq!(get(&app, "/startupz").await, StatusCode::OK);
        assert_eq!(get(&app, "/readyz").await, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(get(&app, "/health").await, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn healthz_ok_before_engine_start() {
        let state = test_state();
        assert!(!state.health.is_job_running());

        let response = healthz_handler(State(state)).await.into_response();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn healthz_ok_when_component_degraded() {
        let state = test_state();
        state.health.mark_degraded("kafka", "reconnecting");

        let response = healthz_handler(State(state)).await.into_response();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn healthz_stays_ok_when_component_is_unhealthy() {
        let state = test_state();
        state.health.register_component("kafka");
        state
            .health
            .mark_unhealthy("kafka", "initialization timed out");

        let response = healthz_handler(State(state)).await.into_response();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn startupz_unavailable_before_engine_start() {
        let state = test_state();

        let response = startupz_handler(State(state)).await.into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn startupz_ok_once_engine_started() {
        let state = test_state();
        state.health.job_started();

        let response = startupz_handler(State(state)).await.into_response();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_ok_when_operational() {
        let state = test_state();
        state.health.register_component("kafka");
        state.health.mark_healthy("kafka");

        let response = readyz_handler(State(state)).await.into_response();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_unavailable_when_component_unhealthy() {
        let state = test_state();
        state.health.register_component("kafka");
        state.health.mark_unhealthy("kafka", "connection lost");

        let response = readyz_handler(State(state)).await.into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn readyz_unavailable_when_readiness_blocked() {
        let state = test_state();
        state.health.register_component("schema");
        state
            .health
            .mark_degraded("schema", "breaking schema change");
        state
            .health
            .block_readiness("schema", "operator action required");

        let response = readyz_handler(State(state.clone())).await.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        // Liveness must stay green while readiness is blocked.
        let response = healthz_handler(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn health_endpoint_still_reports_unhealthy_components() {
        let state = test_state();
        state.health.register_component("kafka");
        state.health.mark_unhealthy("kafka", "connection lost");

        let response = health_handler(State(state)).await.into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

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
