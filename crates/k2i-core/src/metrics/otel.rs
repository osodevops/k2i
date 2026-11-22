//! Optional OpenTelemetry trace export.
//!
//! The exporter is opt-in (`monitoring.otel.enabled`). When it is off, the
//! ingestion path checks [`enabled`] once per instrumented step and creates no
//! spans, so the existing logging and metrics behaviour is unchanged.
//!
//! When it is on, [`init`] builds an OTLP/gRPC exporter and a batch span
//! processor, and [`OtelGuard::layer`] returns the `tracing` layer that turns
//! spans into OpenTelemetry spans. [`OtelGuard::shutdown`] flushes the batch
//! processor so spans produced just before a graceful stop are not lost.

use crate::config::OtelConfig;
use crate::{Error, Result};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing_subscriber::{Layer, Registry};

/// Whether OpenTelemetry span export is active for this process.
///
/// Stored in a process-wide atomic: instrumented call sites do one relaxed
/// load and create no span at all while the feature is off.
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Returns true when the OTel layer is installed.
#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Holds the tracer provider for the lifetime of the process.
///
/// Dropping it does not flush; call [`OtelGuard::shutdown`] on the graceful
/// stop path so buffered spans reach the collector.
pub struct OtelGuard {
    provider: SdkTracerProvider,
}

impl OtelGuard {
    /// Build the `tracing` layer that exports spans to the configured collector.
    pub fn layer(&self) -> impl Layer<Registry> + Send + Sync {
        tracing_opentelemetry::layer::<Registry>().with_tracer(self.provider.tracer("k2i"))
    }

    /// Flush buffered spans and stop the exporter.
    pub fn shutdown(&self) {
        // Reset first so call sites stop creating spans even if flushing
        // stalls or fails.
        ENABLED.store(false, Ordering::Release);
        if let Err(err) = self.provider.shutdown() {
            tracing::warn!(error = %err, "OpenTelemetry provider shutdown failed");
        }
    }
}

/// Build the OTLP exporter and install the process-wide enable flag.
///
/// The caller decides whether to call this at all; it is only invoked when
/// `monitoring.otel.enabled` is true. Building the exporter does not connect
/// to the collector, so a collector that is not reachable yet does not fail
/// startup.
pub fn init(config: &OtelConfig) -> Result<OtelGuard> {
    config.validate()?;

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(config.endpoint.trim())
        .build()
        .map_err(|err| {
            Error::Config(format!(
                "failed to build OTLP span exporter for {}: {err}",
                config.endpoint
            ))
        })?;

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name(config.service_name.trim().to_string())
                .build(),
        )
        .build();

    ENABLED.store(true, Ordering::Release);

    Ok(OtelGuard { provider })
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_sdk::trace::InMemorySpanExporter;
    use tracing_subscriber::prelude::*;

    /// Tests that read or write the process-wide flag must not run in
    /// parallel with each other.
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[test]
    fn disabled_by_default() {
        let _lock = lock();
        assert!(!enabled());
    }

    #[tokio::test]
    async fn init_validates_and_toggles_flag() {
        let _lock = lock();
        let config = OtelConfig {
            enabled: true,
            endpoint: "http://127.0.0.1:4317".to_string(),
            service_name: "k2i-test".to_string(),
        };
        let guard = init(&config).expect("provider should build without a collector");
        assert!(enabled());
        let _layer = guard.layer();
        guard.shutdown();
        assert!(!enabled());
    }

    #[test]
    fn init_rejects_invalid_endpoint() {
        let _lock = lock();
        let config = OtelConfig {
            enabled: true,
            endpoint: "not-a-url".to_string(),
            service_name: "k2i".to_string(),
        };
        assert!(init(&config).is_err());
        assert!(!enabled());
    }

    #[test]
    fn layer_exports_spans() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer::<Registry>().with_tracer(provider.tracer("k2i-test")),
        );
        let _guard = tracing::subscriber::set_default(subscriber);

        {
            let parent = tracing::info_span!("k2i.poll", topic = "orders");
            let _parent = parent.enter();
            let child = tracing::info_span!(parent: &parent, "k2i.decode", row_count = 3_i64);
            child.in_scope(|| {});
        }

        provider.force_flush().expect("flush should succeed");
        let spans = exporter
            .get_finished_spans()
            .expect("in-memory exporter should return spans");
        let names: Vec<_> = spans.iter().map(|span| span.name.as_ref()).collect();
        assert!(names.contains(&"k2i.poll"), "spans: {names:?}");
        assert!(names.contains(&"k2i.decode"), "spans: {names:?}");
        let decode = spans
            .iter()
            .find(|span| span.name.as_ref() == "k2i.decode")
            .expect("decode span");
        assert_eq!(
            decode.parent_span_id,
            spans
                .iter()
                .find(|span| span.name.as_ref() == "k2i.poll")
                .expect("poll span")
                .span_context
                .span_id()
        );
    }
}
