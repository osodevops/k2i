//! Prometheus metrics export.

use prometheus::Registry;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Histogram bucket boundaries in milliseconds for flush duration.
/// Converts to seconds: [0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
const FLUSH_DURATION_BUCKETS_MS: [u64; 7] = [100, 250, 500, 1000, 2500, 5000, 10000];

/// Error types for labeled metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorType {
    /// Kafka-related errors (connection, consume, commit)
    Kafka,
    /// Iceberg/catalog errors (table operations, commits)
    Iceberg,
    /// Object storage errors (S3, GCS, Azure)
    Storage,
    /// Buffer-related errors (overflow, corruption)
    Buffer,
    /// Configuration errors
    Config,
    /// Schema-related errors (evolution, validation)
    Schema,
    /// Unknown/other errors
    Other,
}

impl ErrorType {
    /// Get the label value for Prometheus export.
    pub fn as_label(&self) -> &'static str {
        match self {
            ErrorType::Kafka => "kafka",
            ErrorType::Iceberg => "iceberg",
            ErrorType::Storage => "storage",
            ErrorType::Buffer => "buffer",
            ErrorType::Config => "config",
            ErrorType::Schema => "schema",
            ErrorType::Other => "other",
        }
    }
}

/// Ingestion metrics with counters, gauges, and histograms.
pub struct IngestionMetrics {
    // === COUNTERS ===
    /// Total messages consumed from Kafka
    messages_total: AtomicU64,

    /// Total errors encountered (all types combined)
    errors_total: AtomicU64,

    /// Errors by type
    errors_kafka: AtomicU64,
    errors_iceberg: AtomicU64,
    errors_storage: AtomicU64,
    errors_buffer: AtomicU64,
    errors_config: AtomicU64,
    errors_schema: AtomicU64,
    errors_other: AtomicU64,

    /// Total buffer flushes to Iceberg
    flushes_total: AtomicU64,

    /// Total rows flushed to Iceberg
    rows_flushed_total: AtomicU64,

    /// Total backpressure events
    backpressure_total: AtomicU64,

    /// Total Iceberg commits (snapshots created)
    iceberg_commits_total: AtomicU64,

    // === GAUGES ===
    /// Current hot buffer size in bytes
    buffer_size_bytes: AtomicU64,

    /// Current record count in hot buffer
    buffer_record_count: AtomicU64,

    /// Current Kafka consumer lag (sum across all partitions)
    /// This is the difference between the high watermark and the committed offset
    kafka_consumer_lag: AtomicU64,

    // === HISTOGRAM: flush_duration_seconds ===
    // We track: sum, count, and bucket counts
    /// Sum of all flush durations in milliseconds
    flush_duration_sum_ms: AtomicU64,

    /// Count of flush duration observations
    flush_duration_count: AtomicU64,

    /// Bucket counts for flush duration histogram
    /// Buckets: [0.1s, 0.25s, 0.5s, 1.0s, 2.5s, 5.0s, 10.0s, +Inf]
    flush_duration_buckets: [AtomicU64; 8],

    /// Prometheus registry (optional)
    #[allow(dead_code)]
    registry: Option<Registry>,
}

impl IngestionMetrics {
    /// Create new metrics.
    pub fn new() -> Self {
        Self {
            messages_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            errors_kafka: AtomicU64::new(0),
            errors_iceberg: AtomicU64::new(0),
            errors_storage: AtomicU64::new(0),
            errors_buffer: AtomicU64::new(0),
            errors_config: AtomicU64::new(0),
            errors_schema: AtomicU64::new(0),
            errors_other: AtomicU64::new(0),
            flushes_total: AtomicU64::new(0),
            rows_flushed_total: AtomicU64::new(0),
            backpressure_total: AtomicU64::new(0),
            iceberg_commits_total: AtomicU64::new(0),
            buffer_size_bytes: AtomicU64::new(0),
            buffer_record_count: AtomicU64::new(0),
            kafka_consumer_lag: AtomicU64::new(0),
            flush_duration_sum_ms: AtomicU64::new(0),
            flush_duration_count: AtomicU64::new(0),
            flush_duration_buckets: Default::default(),
            registry: None,
        }
    }

    /// Create metrics with Prometheus registry.
    pub fn with_registry(registry: Registry) -> Self {
        Self {
            messages_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            errors_kafka: AtomicU64::new(0),
            errors_iceberg: AtomicU64::new(0),
            errors_storage: AtomicU64::new(0),
            errors_buffer: AtomicU64::new(0),
            errors_config: AtomicU64::new(0),
            errors_schema: AtomicU64::new(0),
            errors_other: AtomicU64::new(0),
            flushes_total: AtomicU64::new(0),
            rows_flushed_total: AtomicU64::new(0),
            backpressure_total: AtomicU64::new(0),
            iceberg_commits_total: AtomicU64::new(0),
            buffer_size_bytes: AtomicU64::new(0),
            buffer_record_count: AtomicU64::new(0),
            kafka_consumer_lag: AtomicU64::new(0),
            flush_duration_sum_ms: AtomicU64::new(0),
            flush_duration_count: AtomicU64::new(0),
            flush_duration_buckets: Default::default(),
            registry: Some(registry),
        }
    }

    // === COUNTER RECORDING ===

    /// Record a message consumed.
    pub fn record_message(&self) {
        self.messages_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record multiple messages consumed.
    pub fn record_messages(&self, count: u64) {
        self.messages_total.fetch_add(count, Ordering::Relaxed);
    }

    /// Record an error (increments total only, for backward compatibility).
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
        self.errors_other.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error with a specific type label.
    ///
    /// This provides better observability by categorizing errors,
    /// enabling alerts and dashboards per error type.
    pub fn record_error_by_type(&self, error_type: ErrorType) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
        match error_type {
            ErrorType::Kafka => self.errors_kafka.fetch_add(1, Ordering::Relaxed),
            ErrorType::Iceberg => self.errors_iceberg.fetch_add(1, Ordering::Relaxed),
            ErrorType::Storage => self.errors_storage.fetch_add(1, Ordering::Relaxed),
            ErrorType::Buffer => self.errors_buffer.fetch_add(1, Ordering::Relaxed),
            ErrorType::Config => self.errors_config.fetch_add(1, Ordering::Relaxed),
            ErrorType::Schema => self.errors_schema.fetch_add(1, Ordering::Relaxed),
            ErrorType::Other => self.errors_other.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// Record a flush with duration histogram.
    pub fn record_flush(&self, rows: usize, duration: Duration) {
        self.flushes_total.fetch_add(1, Ordering::Relaxed);
        self.rows_flushed_total
            .fetch_add(rows as u64, Ordering::Relaxed);

        // Record duration in histogram
        let duration_ms = duration.as_millis() as u64;
        self.flush_duration_sum_ms
            .fetch_add(duration_ms, Ordering::Relaxed);
        self.flush_duration_count.fetch_add(1, Ordering::Relaxed);

        // Increment appropriate bucket(s)
        // In Prometheus histograms, each bucket is cumulative (le = less than or equal)
        for (i, &bucket_ms) in FLUSH_DURATION_BUCKETS_MS.iter().enumerate() {
            if duration_ms <= bucket_ms {
                self.flush_duration_buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        // +Inf bucket always gets incremented
        self.flush_duration_buckets[7].fetch_add(1, Ordering::Relaxed);
    }

    /// Record a backpressure event.
    pub fn record_backpressure(&self) {
        self.backpressure_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an Iceberg commit (snapshot created).
    pub fn record_iceberg_commit(&self) {
        self.iceberg_commits_total.fetch_add(1, Ordering::Relaxed);
    }

    // === GAUGE UPDATES ===

    /// Update the current buffer size in bytes.
    pub fn set_buffer_size_bytes(&self, size: u64) {
        self.buffer_size_bytes.store(size, Ordering::Relaxed);
    }

    /// Update the current buffer record count.
    pub fn set_buffer_record_count(&self, count: u64) {
        self.buffer_record_count.store(count, Ordering::Relaxed);
    }

    /// Update the current Kafka consumer lag.
    ///
    /// Consumer lag is the difference between the high watermark (latest offset
    /// in the Kafka partition) and the committed offset. A high lag indicates
    /// the consumer is falling behind.
    pub fn set_kafka_consumer_lag(&self, lag: u64) {
        self.kafka_consumer_lag.store(lag, Ordering::Relaxed);
    }

    // === GETTERS ===

    /// Get total messages consumed.
    pub fn messages_total(&self) -> u64 {
        self.messages_total.load(Ordering::Relaxed)
    }

    /// Get total errors.
    pub fn errors_total(&self) -> u64 {
        self.errors_total.load(Ordering::Relaxed)
    }

    /// Get error count by type.
    pub fn errors_by_type(&self, error_type: ErrorType) -> u64 {
        match error_type {
            ErrorType::Kafka => self.errors_kafka.load(Ordering::Relaxed),
            ErrorType::Iceberg => self.errors_iceberg.load(Ordering::Relaxed),
            ErrorType::Storage => self.errors_storage.load(Ordering::Relaxed),
            ErrorType::Buffer => self.errors_buffer.load(Ordering::Relaxed),
            ErrorType::Config => self.errors_config.load(Ordering::Relaxed),
            ErrorType::Schema => self.errors_schema.load(Ordering::Relaxed),
            ErrorType::Other => self.errors_other.load(Ordering::Relaxed),
        }
    }

    /// Get total flushes.
    pub fn flushes_total(&self) -> u64 {
        self.flushes_total.load(Ordering::Relaxed)
    }

    /// Get total rows flushed.
    pub fn rows_flushed_total(&self) -> u64 {
        self.rows_flushed_total.load(Ordering::Relaxed)
    }

    /// Get total backpressure events.
    pub fn backpressure_total(&self) -> u64 {
        self.backpressure_total.load(Ordering::Relaxed)
    }

    /// Get total Iceberg commits.
    pub fn iceberg_commits_total(&self) -> u64 {
        self.iceberg_commits_total.load(Ordering::Relaxed)
    }

    /// Get current buffer size in bytes.
    pub fn buffer_size_bytes(&self) -> u64 {
        self.buffer_size_bytes.load(Ordering::Relaxed)
    }

    /// Get current buffer record count.
    pub fn buffer_record_count(&self) -> u64 {
        self.buffer_record_count.load(Ordering::Relaxed)
    }

    /// Get current Kafka consumer lag.
    pub fn kafka_consumer_lag(&self) -> u64 {
        self.kafka_consumer_lag.load(Ordering::Relaxed)
    }

    /// Get flush duration histogram data for Prometheus export.
    pub fn flush_duration_histogram(&self) -> FlushDurationHistogram {
        FlushDurationHistogram {
            sum_seconds: self.flush_duration_sum_ms.load(Ordering::Relaxed) as f64 / 1000.0,
            count: self.flush_duration_count.load(Ordering::Relaxed),
            buckets: [
                (0.1, self.flush_duration_buckets[0].load(Ordering::Relaxed)),
                (0.25, self.flush_duration_buckets[1].load(Ordering::Relaxed)),
                (0.5, self.flush_duration_buckets[2].load(Ordering::Relaxed)),
                (1.0, self.flush_duration_buckets[3].load(Ordering::Relaxed)),
                (2.5, self.flush_duration_buckets[4].load(Ordering::Relaxed)),
                (5.0, self.flush_duration_buckets[5].load(Ordering::Relaxed)),
                (10.0, self.flush_duration_buckets[6].load(Ordering::Relaxed)),
            ],
            inf_bucket: self.flush_duration_buckets[7].load(Ordering::Relaxed),
        }
    }

    /// Export all metrics in Prometheus text format.
    ///
    /// This produces the standard Prometheus exposition format that can be
    /// scraped by Prometheus or served via an HTTP endpoint.
    ///
    /// # Example
    /// ```
    /// use k2i_core::metrics::IngestionMetrics;
    ///
    /// let metrics = IngestionMetrics::new();
    /// metrics.record_message();
    /// let text = metrics.export_prometheus_text();
    /// assert!(text.contains("k2i_messages_total 1"));
    /// ```
    pub fn export_prometheus_text(&self) -> String {
        let mut output = String::with_capacity(4096);

        // === COUNTERS ===

        output.push_str("# HELP k2i_messages_total Total messages consumed from Kafka\n");
        output.push_str("# TYPE k2i_messages_total counter\n");
        output.push_str(&format!("k2i_messages_total {}\n", self.messages_total()));
        output.push('\n');

        output.push_str("# HELP k2i_errors_total Total errors encountered\n");
        output.push_str("# TYPE k2i_errors_total counter\n");
        output.push_str(&format!("k2i_errors_total {}\n", self.errors_total()));
        output.push('\n');

        output.push_str("# HELP k2i_errors Errors by type\n");
        output.push_str("# TYPE k2i_errors counter\n");
        output.push_str(&format!(
            "k2i_errors{{type=\"kafka\"}} {}\n",
            self.errors_by_type(ErrorType::Kafka)
        ));
        output.push_str(&format!(
            "k2i_errors{{type=\"iceberg\"}} {}\n",
            self.errors_by_type(ErrorType::Iceberg)
        ));
        output.push_str(&format!(
            "k2i_errors{{type=\"storage\"}} {}\n",
            self.errors_by_type(ErrorType::Storage)
        ));
        output.push_str(&format!(
            "k2i_errors{{type=\"buffer\"}} {}\n",
            self.errors_by_type(ErrorType::Buffer)
        ));
        output.push_str(&format!(
            "k2i_errors{{type=\"config\"}} {}\n",
            self.errors_by_type(ErrorType::Config)
        ));
        output.push_str(&format!(
            "k2i_errors{{type=\"schema\"}} {}\n",
            self.errors_by_type(ErrorType::Schema)
        ));
        output.push_str(&format!(
            "k2i_errors{{type=\"other\"}} {}\n",
            self.errors_by_type(ErrorType::Other)
        ));
        output.push('\n');

        output.push_str("# HELP k2i_flushes_total Total buffer flushes to Iceberg\n");
        output.push_str("# TYPE k2i_flushes_total counter\n");
        output.push_str(&format!("k2i_flushes_total {}\n", self.flushes_total()));
        output.push('\n');

        output.push_str("# HELP k2i_rows_flushed_total Total rows flushed to Iceberg\n");
        output.push_str("# TYPE k2i_rows_flushed_total counter\n");
        output.push_str(&format!(
            "k2i_rows_flushed_total {}\n",
            self.rows_flushed_total()
        ));
        output.push('\n');

        output.push_str("# HELP k2i_backpressure_total Total backpressure events\n");
        output.push_str("# TYPE k2i_backpressure_total counter\n");
        output.push_str(&format!(
            "k2i_backpressure_total {}\n",
            self.backpressure_total()
        ));
        output.push('\n');

        output.push_str(
            "# HELP k2i_iceberg_commits_total Total Iceberg commits (snapshots created)\n",
        );
        output.push_str("# TYPE k2i_iceberg_commits_total counter\n");
        output.push_str(&format!(
            "k2i_iceberg_commits_total {}\n",
            self.iceberg_commits_total()
        ));
        output.push('\n');

        // === GAUGES ===

        output.push_str("# HELP k2i_buffer_size_bytes Current hot buffer size in bytes\n");
        output.push_str("# TYPE k2i_buffer_size_bytes gauge\n");
        output.push_str(&format!(
            "k2i_buffer_size_bytes {}\n",
            self.buffer_size_bytes()
        ));
        output.push('\n');

        output.push_str("# HELP k2i_buffer_record_count Current record count in hot buffer\n");
        output.push_str("# TYPE k2i_buffer_record_count gauge\n");
        output.push_str(&format!(
            "k2i_buffer_record_count {}\n",
            self.buffer_record_count()
        ));
        output.push('\n');

        output.push_str(
            "# HELP k2i_kafka_consumer_lag Current Kafka consumer lag (messages behind)\n",
        );
        output.push_str("# TYPE k2i_kafka_consumer_lag gauge\n");
        output.push_str(&format!(
            "k2i_kafka_consumer_lag {}\n",
            self.kafka_consumer_lag()
        ));
        output.push('\n');

        // === HISTOGRAM: flush_duration_seconds ===

        let hist = self.flush_duration_histogram();

        output.push_str("# HELP k2i_flush_duration_seconds Duration of buffer flush operations\n");
        output.push_str("# TYPE k2i_flush_duration_seconds histogram\n");

        // Bucket entries (cumulative, le = less than or equal)
        for (le, count) in &hist.buckets {
            output.push_str(&format!(
                "k2i_flush_duration_seconds_bucket{{le=\"{}\"}} {}\n",
                le, count
            ));
        }
        output.push_str(&format!(
            "k2i_flush_duration_seconds_bucket{{le=\"+Inf\"}} {}\n",
            hist.inf_bucket
        ));

        // Sum and count
        output.push_str(&format!(
            "k2i_flush_duration_seconds_sum {}\n",
            hist.sum_seconds
        ));
        output.push_str(&format!(
            "k2i_flush_duration_seconds_count {}\n",
            hist.count
        ));

        output
    }

    /// Export metrics in Prometheus text format with custom prefix.
    ///
    /// Allows customizing the metric name prefix (default is "k2i").
    pub fn export_prometheus_text_with_prefix(&self, prefix: &str) -> String {
        let mut output = String::with_capacity(4096);

        // === COUNTERS ===

        output.push_str(&format!(
            "# HELP {}_messages_total Total messages consumed from Kafka\n",
            prefix
        ));
        output.push_str(&format!("# TYPE {}_messages_total counter\n", prefix));
        output.push_str(&format!(
            "{}_messages_total {}\n",
            prefix,
            self.messages_total()
        ));
        output.push('\n');

        output.push_str(&format!(
            "# HELP {}_errors_total Total errors encountered\n",
            prefix
        ));
        output.push_str(&format!("# TYPE {}_errors_total counter\n", prefix));
        output.push_str(&format!(
            "{}_errors_total {}\n",
            prefix,
            self.errors_total()
        ));
        output.push('\n');

        output.push_str(&format!("# HELP {}_errors Errors by type\n", prefix));
        output.push_str(&format!("# TYPE {}_errors counter\n", prefix));
        output.push_str(&format!(
            "{}_errors{{type=\"kafka\"}} {}\n",
            prefix,
            self.errors_by_type(ErrorType::Kafka)
        ));
        output.push_str(&format!(
            "{}_errors{{type=\"iceberg\"}} {}\n",
            prefix,
            self.errors_by_type(ErrorType::Iceberg)
        ));
        output.push_str(&format!(
            "{}_errors{{type=\"storage\"}} {}\n",
            prefix,
            self.errors_by_type(ErrorType::Storage)
        ));
        output.push_str(&format!(
            "{}_errors{{type=\"buffer\"}} {}\n",
            prefix,
            self.errors_by_type(ErrorType::Buffer)
        ));
        output.push_str(&format!(
            "{}_errors{{type=\"config\"}} {}\n",
            prefix,
            self.errors_by_type(ErrorType::Config)
        ));
        output.push_str(&format!(
            "{}_errors{{type=\"schema\"}} {}\n",
            prefix,
            self.errors_by_type(ErrorType::Schema)
        ));
        output.push_str(&format!(
            "{}_errors{{type=\"other\"}} {}\n",
            prefix,
            self.errors_by_type(ErrorType::Other)
        ));
        output.push('\n');

        output.push_str(&format!(
            "# HELP {}_flushes_total Total buffer flushes to Iceberg\n",
            prefix
        ));
        output.push_str(&format!("# TYPE {}_flushes_total counter\n", prefix));
        output.push_str(&format!(
            "{}_flushes_total {}\n",
            prefix,
            self.flushes_total()
        ));
        output.push('\n');

        output.push_str(&format!(
            "# HELP {}_rows_flushed_total Total rows flushed to Iceberg\n",
            prefix
        ));
        output.push_str(&format!("# TYPE {}_rows_flushed_total counter\n", prefix));
        output.push_str(&format!(
            "{}_rows_flushed_total {}\n",
            prefix,
            self.rows_flushed_total()
        ));
        output.push('\n');

        output.push_str(&format!(
            "# HELP {}_backpressure_total Total backpressure events\n",
            prefix
        ));
        output.push_str(&format!("# TYPE {}_backpressure_total counter\n", prefix));
        output.push_str(&format!(
            "{}_backpressure_total {}\n",
            prefix,
            self.backpressure_total()
        ));
        output.push('\n');

        output.push_str(&format!(
            "# HELP {}_iceberg_commits_total Total Iceberg commits\n",
            prefix
        ));
        output.push_str(&format!(
            "# TYPE {}_iceberg_commits_total counter\n",
            prefix
        ));
        output.push_str(&format!(
            "{}_iceberg_commits_total {}\n",
            prefix,
            self.iceberg_commits_total()
        ));
        output.push('\n');

        // === GAUGES ===

        output.push_str(&format!(
            "# HELP {}_buffer_size_bytes Current hot buffer size in bytes\n",
            prefix
        ));
        output.push_str(&format!("# TYPE {}_buffer_size_bytes gauge\n", prefix));
        output.push_str(&format!(
            "{}_buffer_size_bytes {}\n",
            prefix,
            self.buffer_size_bytes()
        ));
        output.push('\n');

        output.push_str(&format!(
            "# HELP {}_buffer_record_count Current record count in hot buffer\n",
            prefix
        ));
        output.push_str(&format!("# TYPE {}_buffer_record_count gauge\n", prefix));
        output.push_str(&format!(
            "{}_buffer_record_count {}\n",
            prefix,
            self.buffer_record_count()
        ));
        output.push('\n');

        output.push_str(&format!(
            "# HELP {}_kafka_consumer_lag Current Kafka consumer lag (messages behind)\n",
            prefix
        ));
        output.push_str(&format!("# TYPE {}_kafka_consumer_lag gauge\n", prefix));
        output.push_str(&format!(
            "{}_kafka_consumer_lag {}\n",
            prefix,
            self.kafka_consumer_lag()
        ));
        output.push('\n');

        // === HISTOGRAM ===

        let hist = self.flush_duration_histogram();

        output.push_str(&format!(
            "# HELP {}_flush_duration_seconds Duration of flush operations\n",
            prefix
        ));
        output.push_str(&format!(
            "# TYPE {}_flush_duration_seconds histogram\n",
            prefix
        ));

        for (le, count) in &hist.buckets {
            output.push_str(&format!(
                "{}_flush_duration_seconds_bucket{{le=\"{}\"}} {}\n",
                prefix, le, count
            ));
        }
        output.push_str(&format!(
            "{}_flush_duration_seconds_bucket{{le=\"+Inf\"}} {}\n",
            prefix, hist.inf_bucket
        ));
        output.push_str(&format!(
            "{}_flush_duration_seconds_sum {}\n",
            prefix, hist.sum_seconds
        ));
        output.push_str(&format!(
            "{}_flush_duration_seconds_count {}\n",
            prefix, hist.count
        ));

        output
    }
}

impl Default for IngestionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Flush duration histogram data for export.
#[derive(Debug, Clone)]
pub struct FlushDurationHistogram {
    /// Sum of all observations in seconds
    pub sum_seconds: f64,
    /// Total count of observations
    pub count: u64,
    /// Bucket counts: (le_boundary_seconds, count)
    pub buckets: [(f64, u64); 7],
    /// +Inf bucket count
    pub inf_bucket: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counters() {
        let metrics = IngestionMetrics::new();

        metrics.record_message();
        metrics.record_message();
        assert_eq!(metrics.messages_total(), 2);

        metrics.record_messages(10);
        assert_eq!(metrics.messages_total(), 12);

        metrics.record_error();
        assert_eq!(metrics.errors_total(), 1);
        assert_eq!(metrics.errors_by_type(ErrorType::Other), 1); // record_error() uses Other

        // Test error types
        metrics.record_error_by_type(ErrorType::Kafka);
        metrics.record_error_by_type(ErrorType::Kafka);
        metrics.record_error_by_type(ErrorType::Iceberg);
        metrics.record_error_by_type(ErrorType::Storage);
        assert_eq!(metrics.errors_total(), 5);
        assert_eq!(metrics.errors_by_type(ErrorType::Kafka), 2);
        assert_eq!(metrics.errors_by_type(ErrorType::Iceberg), 1);
        assert_eq!(metrics.errors_by_type(ErrorType::Storage), 1);
        assert_eq!(metrics.errors_by_type(ErrorType::Buffer), 0);
        assert_eq!(metrics.errors_by_type(ErrorType::Config), 0);
        assert_eq!(metrics.errors_by_type(ErrorType::Schema), 0);

        metrics.record_backpressure();
        assert_eq!(metrics.backpressure_total(), 1);

        metrics.record_iceberg_commit();
        assert_eq!(metrics.iceberg_commits_total(), 1);
    }

    #[test]
    fn test_gauges() {
        let metrics = IngestionMetrics::new();

        metrics.set_buffer_size_bytes(1024 * 1024);
        assert_eq!(metrics.buffer_size_bytes(), 1024 * 1024);

        metrics.set_buffer_record_count(1000);
        assert_eq!(metrics.buffer_record_count(), 1000);

        // Update gauge to new value
        metrics.set_buffer_size_bytes(2048 * 1024);
        assert_eq!(metrics.buffer_size_bytes(), 2048 * 1024);

        // Test Kafka consumer lag gauge
        metrics.set_kafka_consumer_lag(5000);
        assert_eq!(metrics.kafka_consumer_lag(), 5000);

        // Update consumer lag to new value
        metrics.set_kafka_consumer_lag(2500);
        assert_eq!(metrics.kafka_consumer_lag(), 2500);
    }

    #[test]
    fn test_flush_with_histogram() {
        let metrics = IngestionMetrics::new();

        // Record a 50ms flush (should be in 0.1s bucket)
        metrics.record_flush(100, Duration::from_millis(50));
        assert_eq!(metrics.flushes_total(), 1);
        assert_eq!(metrics.rows_flushed_total(), 100);

        let hist = metrics.flush_duration_histogram();
        assert_eq!(hist.count, 1);
        assert!((hist.sum_seconds - 0.05).abs() < 0.001);
        // 50ms <= 100ms, so bucket 0 should have 1
        assert_eq!(hist.buckets[0].1, 1);
        assert_eq!(hist.inf_bucket, 1);

        // Record a 500ms flush
        metrics.record_flush(200, Duration::from_millis(500));
        let hist = metrics.flush_duration_histogram();
        assert_eq!(hist.count, 2);
        // Bucket 0 (100ms) should still have 1
        assert_eq!(hist.buckets[0].1, 1);
        // Bucket 2 (500ms) should have 2 (cumulative)
        assert_eq!(hist.buckets[2].1, 2);
    }

    #[test]
    fn test_histogram_buckets() {
        let metrics = IngestionMetrics::new();

        // Record various durations
        metrics.record_flush(10, Duration::from_millis(50)); // <= 0.1s
        metrics.record_flush(10, Duration::from_millis(150)); // <= 0.25s
        metrics.record_flush(10, Duration::from_millis(300)); // <= 0.5s
        metrics.record_flush(10, Duration::from_millis(800)); // <= 1.0s
        metrics.record_flush(10, Duration::from_millis(2000)); // <= 2.5s
        metrics.record_flush(10, Duration::from_millis(4000)); // <= 5.0s
        metrics.record_flush(10, Duration::from_millis(8000)); // <= 10.0s
        metrics.record_flush(10, Duration::from_millis(15000)); // > 10.0s (only +Inf)

        let hist = metrics.flush_duration_histogram();
        assert_eq!(hist.count, 8);

        // Check cumulative bucket counts
        assert_eq!(hist.buckets[0].1, 1); // <= 0.1s
        assert_eq!(hist.buckets[1].1, 2); // <= 0.25s
        assert_eq!(hist.buckets[2].1, 3); // <= 0.5s
        assert_eq!(hist.buckets[3].1, 4); // <= 1.0s
        assert_eq!(hist.buckets[4].1, 5); // <= 2.5s
        assert_eq!(hist.buckets[5].1, 6); // <= 5.0s
        assert_eq!(hist.buckets[6].1, 7); // <= 10.0s
        assert_eq!(hist.inf_bucket, 8); // +Inf (all observations)
    }

    #[test]
    fn test_default() {
        let metrics = IngestionMetrics::default();
        assert_eq!(metrics.messages_total(), 0);
        assert_eq!(metrics.buffer_size_bytes(), 0);
    }

    #[test]
    fn test_prometheus_text_export() {
        let metrics = IngestionMetrics::new();

        // Record some metrics
        metrics.record_messages(100);
        metrics.record_error_by_type(ErrorType::Kafka);
        metrics.record_error_by_type(ErrorType::Kafka);
        metrics.record_error_by_type(ErrorType::Iceberg);
        metrics.record_flush(500, Duration::from_millis(250));
        metrics.record_backpressure();
        metrics.record_iceberg_commit();
        metrics.set_buffer_size_bytes(1024 * 1024);
        metrics.set_buffer_record_count(5000);
        metrics.set_kafka_consumer_lag(12345);

        let output = metrics.export_prometheus_text();

        // Verify counters
        assert!(output.contains("# HELP k2i_messages_total"));
        assert!(output.contains("# TYPE k2i_messages_total counter"));
        assert!(output.contains("k2i_messages_total 100"));

        assert!(output.contains("k2i_errors_total 3"));

        // Verify error type labels
        assert!(output.contains("# HELP k2i_errors Errors by type"));
        assert!(output.contains("# TYPE k2i_errors counter"));
        assert!(output.contains("k2i_errors{type=\"kafka\"} 2"));
        assert!(output.contains("k2i_errors{type=\"iceberg\"} 1"));
        assert!(output.contains("k2i_errors{type=\"storage\"} 0"));
        assert!(output.contains("k2i_flushes_total 1"));
        assert!(output.contains("k2i_rows_flushed_total 500"));
        assert!(output.contains("k2i_backpressure_total 1"));
        assert!(output.contains("k2i_iceberg_commits_total 1"));

        // Verify gauges
        assert!(output.contains("# TYPE k2i_buffer_size_bytes gauge"));
        assert!(output.contains("k2i_buffer_size_bytes 1048576"));
        assert!(output.contains("k2i_buffer_record_count 5000"));
        assert!(output.contains("# TYPE k2i_kafka_consumer_lag gauge"));
        assert!(output.contains("k2i_kafka_consumer_lag 12345"));

        // Verify histogram
        assert!(output.contains("# TYPE k2i_flush_duration_seconds histogram"));
        assert!(output.contains("k2i_flush_duration_seconds_bucket{le=\"0.1\"}")); // 100ms bucket
        assert!(output.contains("k2i_flush_duration_seconds_bucket{le=\"+Inf\"} 1"));
        assert!(output.contains("k2i_flush_duration_seconds_sum 0.25"));
        assert!(output.contains("k2i_flush_duration_seconds_count 1"));
    }

    #[test]
    fn test_prometheus_text_export_with_prefix() {
        let metrics = IngestionMetrics::new();
        metrics.record_messages(42);

        let output = metrics.export_prometheus_text_with_prefix("custom_app");

        assert!(output.contains("# HELP custom_app_messages_total"));
        assert!(output.contains("custom_app_messages_total 42"));
        assert!(output.contains("custom_app_buffer_size_bytes"));
        assert!(output.contains("custom_app_flush_duration_seconds_bucket"));
    }

    #[test]
    fn test_prometheus_text_export_empty() {
        let metrics = IngestionMetrics::new();
        let output = metrics.export_prometheus_text();

        // All counters should be 0
        assert!(output.contains("k2i_messages_total 0"));
        assert!(output.contains("k2i_errors_total 0"));
        assert!(output.contains("k2i_flushes_total 0"));

        // Histogram should have zero count
        assert!(output.contains("k2i_flush_duration_seconds_count 0"));
        assert!(output.contains("k2i_flush_duration_seconds_sum 0"));
    }

    #[test]
    fn test_prometheus_text_format_validity() {
        let metrics = IngestionMetrics::new();
        metrics.record_flush(100, Duration::from_millis(150));

        let output = metrics.export_prometheus_text();

        // Check that each metric line follows Prometheus format:
        // - # HELP lines have metric name and description
        // - # TYPE lines have metric name and type
        // - Metric lines have name, optional labels, and value
        for line in output.lines() {
            if line.starts_with("# HELP ") {
                assert!(line.contains("k2i_"));
            } else if line.starts_with("# TYPE ") {
                assert!(
                    line.contains("counter")
                        || line.contains("gauge")
                        || line.contains("histogram")
                );
            } else if !line.is_empty() {
                // Metric line should start with k2i_ or contain a value
                assert!(line.starts_with("k2i_"));
            }
        }
    }
}
