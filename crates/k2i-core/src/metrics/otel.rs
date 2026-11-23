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
//!
//! With `monitoring.otel.correlate_kafka_trace_context` on (the default), the
//! W3C `traceparent` header of a consumed Kafka record parents a `k2i.message`
//! span to the producer's trace. The batch decode, flush, and commit spans link
//! to those message spans instead of nesting under one arbitrary producer.

use crate::config::OtelConfig;
use crate::kafka::KafkaMessage;
use crate::{Error, Result};
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use opentelemetry::Context as OtelContext;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{Layer, Registry};

/// Whether OpenTelemetry span export is active for this process.
///
/// Stored in a process-wide atomic: instrumented call sites do one relaxed
/// load and create no span at all while the feature is off.
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Whether producer trace context from Kafka headers is attached to spans.
static CORRELATE: AtomicBool = AtomicBool::new(false);

/// Maximum number of trace links attached to a single span.
///
/// A flush can aggregate rows from many producer traces; the cap keeps one
/// abandoned or fan-out producer from producing unbounded span metadata.
/// Distinct traces beyond the cap are counted in the `dropped_trace_links`
/// span attribute.
pub const MAX_SPAN_TRACE_LINKS: usize = 32;

/// Returns true when the OTel layer is installed.
#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Returns true when spans are exported and producer trace context is used.
///
/// Call sites check this once per batch; with tracing off it is a single
/// relaxed load and no header is read.
#[inline]
pub fn correlate_kafka_trace_context() -> bool {
    enabled() && CORRELATE.load(Ordering::Relaxed)
}

/// A compact reference to an OpenTelemetry span: the 16-byte trace id plus the
/// 8-byte span id.
///
/// One is stored per buffered row that carried a producer context, so a flush
/// that happens much later can link back to the traces that produced its rows.
/// Only the ids are kept: `TraceState` and other per-message metadata would
/// allocate on the hot path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TraceLink {
    trace_id: [u8; 16],
    span_id: [u8; 8],
}

impl TraceLink {
    /// Build a link from a valid span context, or `None` when it is unusable.
    pub fn from_span_context(context: &SpanContext) -> Option<Self> {
        if !context.is_valid() {
            return None;
        }
        Some(Self {
            trace_id: context.trace_id().to_bytes(),
            span_id: context.span_id().to_bytes(),
        })
    }

    /// The W3C trace id.
    pub fn trace_id(&self) -> TraceId {
        TraceId::from_bytes(self.trace_id)
    }

    /// The span id.
    pub fn span_id(&self) -> SpanId {
        SpanId::from_bytes(self.span_id)
    }

    /// Rebuild a span context for use as an OpenTelemetry link.
    pub fn span_context(&self) -> SpanContext {
        SpanContext::new(
            self.trace_id(),
            self.span_id(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        )
    }
}

/// Reads W3C trace-context fields out of Kafka message headers.
///
/// Header names are matched case-insensitively (HTTP header semantics) and a
/// non-UTF-8 value is reported as absent, so a binary header on the same name
/// can never panic the consumer.
struct HeaderExtractor<'a> {
    headers: &'a [(String, Vec<u8>)],
}

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(key))
            .and_then(|(_, value)| std::str::from_utf8(value).ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.headers.iter().map(|(name, _)| name.as_str()).collect()
    }
}

/// Extract the producer's W3C trace context from message headers.
///
/// Returns `None` for a missing, empty, malformed, foreign, or unsampled
/// `traceparent`; a `tracestate` is carried through when present. Correlation
/// is best effort: a bad header never fails ingestion.
pub fn extract_span_context(headers: &[(String, Vec<u8>)]) -> Option<SpanContext> {
    if headers.is_empty() {
        return None;
    }
    let propagator = TraceContextPropagator::new();
    let context = propagator.extract(&HeaderExtractor { headers });
    let span_context = context.span().span_context().clone();
    // The propagator already masks unknown trace flags; only a sampled remote
    // parent is worth a span, since an unsampled producer trace would never
    // store it.
    if span_context.is_valid() && span_context.is_sampled() {
        Some(span_context)
    } else {
        None
    }
}

/// Create the `k2i.message` span for one consumed record.
fn start_message_span(message: &KafkaMessage) -> Span {
    tracing::info_span!(
        "k2i.message",
        topic = %message.topic,
        partition = message.partition,
        offset = message.offset,
    )
}

/// Parent one message span to its producer trace and return its link.
fn correlate_message(message: &KafkaMessage) -> Option<TraceLink> {
    let parent = extract_span_context(&message.headers)?;
    let span = start_message_span(message);
    // `set_parent` must run before the span is first used; this is the first
    // thing that happens to it.
    if span
        .set_parent(OtelContext::new().with_remote_span_context(parent))
        .is_err()
    {
        // No OTel layer is installed (or it filtered the span); reporting a
        // link whose target was never exported would be misleading.
        return None;
    }
    // Building the span here starts it with the remote parent; it ends when
    // `span` is dropped at the end of this function.
    let context = span.context();
    TraceLink::from_span_context(context.span().span_context())
}

/// Create a `k2i.message` span for every message that carries a producer trace
/// context and return one link per traced message, aligned to `messages`.
///
/// The returned vector is empty when no message carries a usable context, so
/// the untraced path allocates nothing.
pub(crate) fn correlate_messages(messages: &[KafkaMessage]) -> Vec<Option<TraceLink>> {
    let mut links: Vec<Option<TraceLink>> = Vec::new();
    for (index, message) in messages.iter().enumerate() {
        match correlate_message(message) {
            Some(link) => {
                if links.is_empty() {
                    links.resize(index, None);
                }
                links.push(Some(link));
            }
            None => {
                if !links.is_empty() {
                    links.push(None);
                }
            }
        }
    }
    links
}

/// The bounded set of links attached to a span, plus the number of distinct
/// traces that did not fit.
#[derive(Debug, Default)]
pub(crate) struct TraceLinkSet {
    contexts: Vec<SpanContext>,
    dropped: usize,
}

impl TraceLinkSet {
    /// Collect at most one link per distinct trace id, up to `max`.
    pub(crate) fn collect<'a>(links: impl IntoIterator<Item = &'a TraceLink>, max: usize) -> Self {
        let mut contexts = Vec::new();
        let mut seen = HashSet::new();
        let mut dropped = 0;
        for link in links {
            if !seen.insert(link.trace_id) {
                continue;
            }
            if contexts.len() < max {
                contexts.push(link.span_context());
            } else {
                dropped += 1;
            }
        }
        Self { contexts, dropped }
    }

    /// Attach every collected link to `span`.
    pub(crate) fn attach(&self, span: &Span) {
        for context in &self.contexts {
            span.add_link(context.clone());
        }
    }

    /// Number of distinct traces that did not become links.
    pub(crate) fn dropped(&self) -> usize {
        self.dropped
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.contexts.len()
    }
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
        CORRELATE.store(false, Ordering::Release);
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

    CORRELATE.store(config.correlate_kafka_trace_context, Ordering::Release);
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

    /// A producer `traceparent` used across the correlation tests.
    const TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const TRACE_ID: [u8; 16] = [
        0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47,
        0x36,
    ];
    const PARENT_SPAN_ID: u64 = 0x00f0_67aa_0ba9_02b7;

    fn message(offset: i64, traceparent: Option<&str>) -> KafkaMessage {
        KafkaMessage {
            key: None,
            value: Some(b"row".to_vec()),
            topic: "orders".to_string(),
            partition: 0,
            offset,
            timestamp: 0,
            headers: traceparent
                .map(|value| vec![("traceparent".to_string(), value.as_bytes().to_vec())])
                .unwrap_or_default(),
        }
    }

    fn memory_provider() -> (InMemorySpanExporter, SdkTracerProvider) {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter.clone())
            .build();
        (exporter, provider)
    }

    fn install(provider: &SdkTracerProvider) -> tracing::subscriber::DefaultGuard {
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer::<Registry>().with_tracer(provider.tracer("k2i-test")),
        );
        tracing::subscriber::set_default(subscriber)
    }

    #[test]
    fn disabled_by_default() {
        let _lock = lock();
        assert!(!enabled());
        assert!(!correlate_kafka_trace_context());
    }

    #[tokio::test]
    async fn init_validates_and_toggles_flag() {
        let _lock = lock();
        let config = OtelConfig {
            enabled: true,
            correlate_kafka_trace_context: true,
            ..OtelConfig::default()
        };
        let guard = init(&config).expect("provider should build without a collector");
        assert!(enabled());
        assert!(correlate_kafka_trace_context());
        let _layer = guard.layer();
        guard.shutdown();
        assert!(!enabled());
        assert!(!correlate_kafka_trace_context());
    }

    #[tokio::test]
    async fn init_honours_disabled_correlation() {
        let _lock = lock();
        let config = OtelConfig {
            enabled: true,
            endpoint: "http://127.0.0.1:4317".to_string(),
            correlate_kafka_trace_context: false,
            ..OtelConfig::default()
        };
        let guard = init(&config).expect("provider should build without a collector");
        assert!(enabled());
        assert!(!correlate_kafka_trace_context());
        guard.shutdown();
    }

    #[test]
    fn init_rejects_invalid_endpoint() {
        let _lock = lock();
        let config = OtelConfig {
            enabled: true,
            endpoint: "not-a-url".to_string(),
            ..OtelConfig::default()
        };
        assert!(init(&config).is_err());
        assert!(!enabled());
    }

    #[test]
    fn correlation_flag_env_override() {
        assert!(OtelConfig::default().correlate_kafka_trace_context);
        std::env::set_var("K2I_MONITORING_OTEL_CORRELATE_KAFKA_TRACE_CONTEXT", "false");
        let mut config = OtelConfig::default();
        config.apply_env_overrides();
        std::env::remove_var("K2I_MONITORING_OTEL_CORRELATE_KAFKA_TRACE_CONTEXT");
        assert!(!config.correlate_kafka_trace_context);
    }

    #[test]
    fn extracts_valid_traceparent_and_tracestate() {
        let headers = vec![
            ("traceparent".to_string(), TRACEPARENT.as_bytes().to_vec()),
            ("tracestate".to_string(), b"vendor=opaque".to_vec()),
        ];
        let context = extract_span_context(&headers).expect("valid traceparent");
        assert_eq!(context.trace_id().to_bytes(), TRACE_ID);
        assert_eq!(context.span_id(), SpanId::from(PARENT_SPAN_ID));
        assert!(context.is_sampled());
        assert_eq!(context.trace_state().header(), "vendor=opaque");

        // HTTP header names are case-insensitive; Kafka keeps the producer's case.
        let upper = vec![("TraceParent".to_string(), TRACEPARENT.as_bytes().to_vec())];
        assert!(extract_span_context(&upper).is_some());
    }

    #[test]
    fn ignores_missing_malformed_empty_and_unsampled_traceparent() {
        assert!(extract_span_context(&[]).is_none());

        for value in [
            "",
            "garbage",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ] {
            let headers = vec![("traceparent".to_string(), value.as_bytes().to_vec())];
            assert!(
                extract_span_context(&headers).is_none(),
                "should ignore traceparent: {value:?}"
            );
        }

        let binary = vec![("traceparent".to_string(), vec![0xff, 0xfe])];
        assert!(extract_span_context(&binary).is_none());
    }

    #[test]
    fn traced_message_span_is_parented_and_decode_links_to_it() {
        let (exporter, provider) = memory_provider();
        let _subscriber = install(&provider);

        let messages = vec![message(7, Some(TRACEPARENT)), message(8, None)];
        let links = correlate_messages(&messages);
        assert_eq!(links.len(), messages.len());
        assert!(links[1].is_none(), "untraced message must not link");
        let link = links[0].expect("traced message produces a link");

        let decode = tracing::info_span!(
            "k2i.decode",
            row_count = 2_i64,
            dropped_trace_links = tracing::field::Empty,
        );
        let set = TraceLinkSet::collect(links.iter().flatten(), MAX_SPAN_TRACE_LINKS);
        assert_eq!(set.len(), 1);
        set.attach(&decode);
        decode.record("dropped_trace_links", set.dropped() as i64);
        drop(decode);

        provider.force_flush().expect("flush should succeed");
        let spans = exporter
            .get_finished_spans()
            .expect("in-memory exporter should return spans");

        let message_span = spans
            .iter()
            .find(|span| span.name.as_ref() == "k2i.message")
            .expect("message span");
        assert_eq!(
            message_span.parent_span_id,
            SpanId::from(PARENT_SPAN_ID),
            "message span must be a child of the producer span"
        );
        assert_eq!(message_span.span_context.trace_id().to_bytes(), TRACE_ID);
        for attribute in ["topic", "partition", "offset"] {
            assert!(
                message_span
                    .attributes
                    .iter()
                    .any(|kv| kv.key.as_str() == attribute),
                "message span should carry {attribute}"
            );
        }
        assert_eq!(link.trace_id().to_bytes(), TRACE_ID);
        assert_eq!(link.span_id(), message_span.span_context.span_id());

        let decode_span = spans
            .iter()
            .find(|span| span.name.as_ref() == "k2i.decode")
            .expect("decode span");
        assert_eq!(decode_span.links.links.len(), 1);
        assert_eq!(
            decode_span.links.links[0]
                .span_context
                .trace_id()
                .to_bytes(),
            TRACE_ID
        );
        assert_eq!(
            decode_span.links.links[0].span_context.span_id(),
            message_span.span_context.span_id(),
            "decode must link to the message span, not the producer span"
        );
        assert_eq!(
            decode_span.parent_span_id,
            SpanId::INVALID,
            "batch span is not nested under an arbitrary producer"
        );
    }

    #[test]
    fn flush_links_are_deduplicated_by_trace_and_bounded() {
        let (exporter, provider) = memory_provider();
        let _subscriber = install(&provider);

        let other_traceparent = "00-11111111111111111111111111111111-2222222222222222-01";
        let messages = vec![
            message(1, Some(TRACEPARENT)),
            message(2, Some(TRACEPARENT)),
            message(3, Some(other_traceparent)),
        ];
        let links = correlate_messages(&messages);

        let flush = tracing::info_span!(
            "k2i.flush",
            batch_id = 1_i64,
            dropped_trace_links = tracing::field::Empty,
        );
        let set = TraceLinkSet::collect(links.iter().flatten(), 1);
        assert_eq!(set.len(), 1, "one link per distinct trace");
        assert_eq!(set.dropped(), 1, "the second distinct trace is counted");
        set.attach(&flush);
        flush.record("dropped_trace_links", set.dropped() as i64);
        drop(flush);

        provider.force_flush().expect("flush should succeed");
        let spans = exporter
            .get_finished_spans()
            .expect("in-memory exporter should return spans");
        let flush_span = spans
            .iter()
            .find(|span| span.name.as_ref() == "k2i.flush")
            .expect("flush span");
        assert_eq!(flush_span.links.links.len(), 1);
        assert_eq!(
            flush_span.links.links[0].span_context.trace_id().to_bytes(),
            TRACE_ID
        );
        let dropped = flush_span
            .attributes
            .iter()
            .find(|kv| kv.key.as_str() == "dropped_trace_links")
            .expect("dropped_trace_links attribute");
        assert_eq!(dropped.value, opentelemetry::Value::I64(1));
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
