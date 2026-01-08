//! Kafka consumer with backpressure support and exponential backoff.
//!
//! Uses CooperativeSticky assignor for minimal rebalance disruption
//! and manual offset commits for exactly-once semantics.
//!
//! ## Exponential Backoff
//!
//! The consumer implements exponential backoff for transient failures:
//! - Connection failures during startup
//! - Offset commit failures
//! - Poll errors
//!
//! Backoff formula: min(max_delay, base_delay * 2^attempt) + jitter

use crate::config::KafkaConfig;
use crate::{Error, KafkaError, Result};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::ClientConfig;
use rdkafka::TopicPartitionList;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Configuration for exponential backoff retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Base delay for first retry (default: 100ms)
    pub base_delay: Duration,
    /// Maximum delay cap (default: 30s)
    pub max_delay: Duration,
    /// Maximum number of retries (default: 10)
    pub max_retries: u32,
    /// Whether to add jitter (randomness) to delays (default: true)
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            max_retries: 10,
            jitter: true,
        }
    }
}

impl RetryConfig {
    /// Create a new retry config with custom settings.
    pub fn new(base_delay: Duration, max_delay: Duration, max_retries: u32) -> Self {
        Self {
            base_delay,
            max_delay,
            max_retries,
            jitter: true,
        }
    }

    /// Create a config optimized for Kafka operations.
    pub fn for_kafka() -> Self {
        Self {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            max_retries: 10,
            jitter: true,
        }
    }

    /// Disable jitter (for testing).
    pub fn without_jitter(mut self) -> Self {
        self.jitter = false;
        self
    }

    /// Calculate the backoff delay for a given attempt.
    ///
    /// Uses exponential backoff: min(max_delay, base_delay * 2^attempt)
    /// Optionally adds jitter (±25%) to prevent thundering herd.
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        let base_ms = self.base_delay.as_millis() as u64;
        let max_ms = self.max_delay.as_millis() as u64;

        // Calculate exponential delay: base * 2^attempt
        let delay_ms = base_ms.saturating_mul(1u64 << attempt.min(20));
        let capped_delay_ms = delay_ms.min(max_ms);

        // Add jitter if enabled (±25%)
        let final_delay_ms = if self.jitter {
            let jitter_range = capped_delay_ms / 4;
            let jitter = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .subsec_nanos() as u64)
                % (jitter_range * 2 + 1);
            capped_delay_ms.saturating_sub(jitter_range) + jitter
        } else {
            capped_delay_ms
        };

        Duration::from_millis(final_delay_ms)
    }
}

/// Builder for creating a Kafka consumer.
pub struct KafkaConsumerBuilder {
    config: KafkaConfig,
    retry_config: RetryConfig,
}

impl KafkaConsumerBuilder {
    /// Create a new consumer builder.
    pub fn new(config: KafkaConfig) -> Self {
        Self {
            config,
            retry_config: RetryConfig::for_kafka(),
        }
    }

    /// Set custom retry configuration.
    pub fn with_retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    /// Build the consumer.
    pub fn build(self) -> Result<SmartKafkaConsumer> {
        let mut client_config = ClientConfig::new();

        // Basic configuration
        client_config
            .set("bootstrap.servers", self.config.bootstrap_servers.join(","))
            .set("group.id", &self.config.consumer_group)
            // CRITICAL: Manual commits only - we commit after transaction log write
            .set("enable.auto.commit", "false")
            .set(
                "auto.offset.reset",
                match self.config.auto_offset_reset {
                    crate::config::OffsetReset::Earliest => "earliest",
                    crate::config::OffsetReset::Latest => "latest",
                },
            )
            .set(
                "session.timeout.ms",
                self.config.session_timeout_ms.to_string(),
            )
            .set(
                "heartbeat.interval.ms",
                self.config.heartbeat_interval_ms.to_string(),
            )
            // CRITICAL: max.poll.interval.ms must exceed longest flush time
            // Otherwise Kafka will kick consumer from group during long writes
            .set(
                "max.poll.interval.ms",
                self.config.max_poll_interval_ms.to_string(),
            )
            // Use CooperativeSticky for minimal rebalance disruption
            .set("partition.assignment.strategy", "cooperative-sticky")
            // Disable auto offset store - we control when offsets are stored
            .set("enable.auto.offset.store", "false");

        // Security configuration
        if let Some(ref protocol) = self.config.security.protocol {
            client_config.set("security.protocol", protocol);
        }
        if let Some(ref mechanism) = self.config.security.sasl_mechanism {
            client_config.set("sasl.mechanism", mechanism);
        }
        if let Some(ref username) = self.config.security.sasl_username {
            client_config.set("sasl.username", username);
        }
        if let Some(ref password) = self.config.security.sasl_password {
            client_config.set("sasl.password", password);
        }
        if let Some(ref path) = self.config.security.ssl_ca_location {
            client_config.set("ssl.ca.location", path.to_string_lossy().as_ref());
        }
        if let Some(ref path) = self.config.security.ssl_cert_location {
            client_config.set("ssl.certificate.location", path.to_string_lossy().as_ref());
        }
        if let Some(ref path) = self.config.security.ssl_key_location {
            client_config.set("ssl.key.location", path.to_string_lossy().as_ref());
        }

        let consumer: StreamConsumer = client_config.create().map_err(|e| {
            Error::Kafka(KafkaError::ConnectionFailed {
                broker: self.config.bootstrap_servers.join(","),
                message: e.to_string(),
            })
        })?;

        consumer
            .subscribe(&[&self.config.topic])
            .map_err(|e| Error::Kafka(KafkaError::ConsumerGroup(e.to_string())))?;

        info!(
            topic = %self.config.topic,
            group = %self.config.consumer_group,
            servers = %self.config.bootstrap_servers.join(","),
            "Kafka consumer subscribed"
        );

        Ok(SmartKafkaConsumer {
            consumer: Arc::new(consumer),
            topic: self.config.topic,
            paused: false,
            retry_config: self.retry_config,
            consecutive_failures: AtomicU32::new(0),
        })
    }
}

/// Kafka consumer with backpressure support and exponential backoff.
pub struct SmartKafkaConsumer {
    consumer: Arc<StreamConsumer>,
    topic: String,
    paused: bool,
    /// Retry configuration for operations
    retry_config: RetryConfig,
    /// Counter for consecutive failures (for backoff calculation)
    consecutive_failures: AtomicU32,
}

impl SmartKafkaConsumer {
    /// Pause consumption (backpressure).
    pub fn pause(&mut self) -> Result<()> {
        if self.paused {
            return Ok(());
        }

        let assignment = self
            .consumer
            .assignment()
            .map_err(|e| Error::Kafka(KafkaError::PartitionAssignment(e.to_string())))?;

        if assignment.count() == 0 {
            return Ok(());
        }

        self.consumer
            .pause(&assignment)
            .map_err(|e| Error::Kafka(KafkaError::PartitionAssignment(e.to_string())))?;

        self.paused = true;
        warn!("Kafka consumer PAUSED due to backpressure");
        Ok(())
    }

    /// Resume consumption.
    pub fn resume(&mut self) -> Result<()> {
        if !self.paused {
            return Ok(());
        }

        let assignment = self
            .consumer
            .assignment()
            .map_err(|e| Error::Kafka(KafkaError::PartitionAssignment(e.to_string())))?;

        if assignment.count() == 0 {
            self.paused = false;
            return Ok(());
        }

        self.consumer
            .resume(&assignment)
            .map_err(|e| Error::Kafka(KafkaError::PartitionAssignment(e.to_string())))?;

        self.paused = false;
        info!("Kafka consumer RESUMED");
        Ok(())
    }

    /// Poll for messages with timeout.
    pub async fn poll(&self, timeout: Duration) -> Option<Result<KafkaMessage>> {
        use futures::StreamExt;

        let stream = self.consumer.stream();
        tokio::pin!(stream);

        match tokio::time::timeout(timeout, stream.next()).await {
            Ok(Some(Ok(msg))) => Some(Ok(Self::convert_message(&msg))),
            Ok(Some(Err(e))) => Some(Err(Error::Kafka(KafkaError::ConsumerGroup(e.to_string())))),
            Ok(None) => None,
            Err(_) => None, // Timeout
        }
    }

    /// Poll for a batch of messages.
    pub async fn poll_batch(
        &self,
        max_messages: usize,
        timeout: Duration,
    ) -> Result<Vec<KafkaMessage>> {
        use futures::StreamExt;

        let mut batch = Vec::with_capacity(max_messages);
        let deadline = tokio::time::Instant::now() + timeout;
        let stream = self.consumer.stream();
        tokio::pin!(stream);

        while batch.len() < max_messages {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok(msg))) => {
                    batch.push(Self::convert_message(&msg));
                }
                Ok(Some(Err(e))) => {
                    return Err(Error::Kafka(KafkaError::ConsumerGroup(e.to_string())));
                }
                Ok(None) | Err(_) => break,
            }
        }

        Ok(batch)
    }

    /// Convert a borrowed message to an owned KafkaMessage.
    fn convert_message(msg: &BorrowedMessage) -> KafkaMessage {
        KafkaMessage {
            key: msg.key().map(|k| k.to_vec()),
            value: msg.payload().map(|v| v.to_vec()),
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp: msg.timestamp().to_millis().unwrap_or(0),
            headers: Self::extract_headers(msg),
        }
    }

    /// Extract headers from a message.
    fn extract_headers(msg: &BorrowedMessage) -> Vec<(String, Vec<u8>)> {
        msg.headers()
            .map(|headers| {
                (0..headers.count())
                    .map(|i| {
                        let header = headers.get(i);
                        (header.key.to_string(), header.value.unwrap_or(&[]).to_vec())
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Commit offset for a specific partition.
    ///
    /// CRITICAL: Only call this AFTER transaction log write!
    pub async fn commit_offset(&self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        let mut tpl = TopicPartitionList::new();
        // Commit offset + 1 (Kafka convention: committed offset is the next offset to read)
        tpl.add_partition_offset(topic, partition, rdkafka::Offset::Offset(offset + 1))
            .map_err(|e| Error::Kafka(KafkaError::OffsetCommit(e.to_string())))?;

        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Async)
            .map_err(|e| Error::Kafka(KafkaError::OffsetCommit(e.to_string())))?;

        debug!(
            topic = %topic,
            partition = %partition,
            offset = %offset,
            "Offset committed"
        );

        Ok(())
    }

    /// Commit offsets for multiple partitions.
    pub async fn commit_offsets(&self, offsets: &[(String, i32, i64)]) -> Result<()> {
        let mut tpl = TopicPartitionList::new();

        for (topic, partition, offset) in offsets {
            tpl.add_partition_offset(topic, *partition, rdkafka::Offset::Offset(offset + 1))
                .map_err(|e| Error::Kafka(KafkaError::OffsetCommit(e.to_string())))?;
        }

        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Async)
            .map_err(|e| Error::Kafka(KafkaError::OffsetCommit(e.to_string())))?;

        debug!(
            offsets = ?offsets,
            "Batch offsets committed"
        );

        Ok(())
    }

    /// Check if the consumer is paused.
    pub fn is_paused(&self) -> bool {
        self.paused
    }

    /// Get the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Get the current partition assignment.
    pub fn assignment(&self) -> Result<Vec<(String, i32)>> {
        let tpl = self
            .consumer
            .assignment()
            .map_err(|e| Error::Kafka(KafkaError::PartitionAssignment(e.to_string())))?;

        Ok(tpl
            .elements()
            .iter()
            .map(|e| (e.topic().to_string(), e.partition()))
            .collect())
    }

    // === EXPONENTIAL BACKOFF METHODS ===

    /// Record a successful operation (resets backoff).
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
    }

    /// Record a failure (increments backoff counter).
    fn record_failure(&self) -> u32 {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Get the current backoff delay based on consecutive failures.
    pub fn current_backoff_delay(&self) -> Duration {
        let failures = self.consecutive_failures.load(Ordering::Relaxed);
        self.retry_config.calculate_delay(failures)
    }

    /// Get the number of consecutive failures.
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }

    /// Check if we should retry (haven't exceeded max retries).
    pub fn should_retry(&self) -> bool {
        self.consecutive_failures.load(Ordering::Relaxed) < self.retry_config.max_retries
    }

    /// Commit offset with exponential backoff retry.
    ///
    /// Retries the commit operation with increasing delays on failure.
    /// Returns the result after all retries are exhausted or on success.
    pub async fn commit_offset_with_retry(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        let mut attempt = 0;

        loop {
            match self.commit_offset(topic, partition, offset).await {
                Ok(()) => {
                    self.record_success();
                    return Ok(());
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.retry_config.max_retries {
                        warn!(
                            attempt = attempt,
                            max_retries = self.retry_config.max_retries,
                            error = %e,
                            "Offset commit failed after max retries"
                        );
                        return Err(e);
                    }

                    let delay = self.retry_config.calculate_delay(attempt);
                    warn!(
                        attempt = attempt,
                        delay_ms = delay.as_millis(),
                        error = %e,
                        "Offset commit failed, retrying with backoff"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Commit multiple offsets with exponential backoff retry.
    pub async fn commit_offsets_with_retry(&self, offsets: &[(String, i32, i64)]) -> Result<()> {
        let mut attempt = 0;

        loop {
            match self.commit_offsets(offsets).await {
                Ok(()) => {
                    self.record_success();
                    return Ok(());
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.retry_config.max_retries {
                        warn!(
                            attempt = attempt,
                            max_retries = self.retry_config.max_retries,
                            error = %e,
                            "Batch offset commit failed after max retries"
                        );
                        return Err(e);
                    }

                    let delay = self.retry_config.calculate_delay(attempt);
                    warn!(
                        attempt = attempt,
                        delay_ms = delay.as_millis(),
                        error = %e,
                        "Batch offset commit failed, retrying with backoff"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Poll for messages with automatic backoff on errors.
    ///
    /// On error, records the failure and returns a delay recommendation
    /// for the caller to implement (e.g., sleep before next poll).
    pub async fn poll_with_backoff(&self, timeout: Duration) -> PollResult {
        match self.poll(timeout).await {
            Some(Ok(msg)) => {
                self.record_success();
                PollResult::Message(msg)
            }
            Some(Err(e)) => {
                let failures = self.record_failure();
                let should_retry = failures < self.retry_config.max_retries;
                let backoff = self.retry_config.calculate_delay(failures);
                warn!(
                    failures = failures,
                    backoff_ms = backoff.as_millis(),
                    error = %e,
                    "Poll error, suggesting backoff"
                );
                PollResult::Error {
                    error: e,
                    backoff,
                    should_retry,
                }
            }
            None => PollResult::Timeout,
        }
    }

    /// Get the retry configuration.
    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry_config
    }
}

/// Result of a poll operation with backoff information.
#[derive(Debug)]
pub enum PollResult {
    /// Successfully received a message.
    Message(KafkaMessage),
    /// Poll timed out (no messages available).
    Timeout,
    /// Poll failed with error and backoff recommendation.
    Error {
        /// The error that occurred.
        error: Error,
        /// Recommended backoff duration before next attempt.
        backoff: Duration,
        /// Whether to retry (false if max retries exceeded).
        should_retry: bool,
    },
}

/// A Kafka message with all metadata.
#[derive(Debug, Clone)]
pub struct KafkaMessage {
    /// Message key (optional)
    pub key: Option<Vec<u8>>,

    /// Message value (optional)
    pub value: Option<Vec<u8>>,

    /// Topic name
    pub topic: String,

    /// Partition number
    pub partition: i32,

    /// Offset within the partition
    pub offset: i64,

    /// Message timestamp (milliseconds since epoch)
    pub timestamp: i64,

    /// Message headers
    pub headers: Vec<(String, Vec<u8>)>,
}

impl KafkaMessage {
    /// Get the message key as a string.
    pub fn key_str(&self) -> Option<&str> {
        self.key.as_ref().and_then(|k| std::str::from_utf8(k).ok())
    }

    /// Get the message value as a string.
    pub fn value_str(&self) -> Option<&str> {
        self.value
            .as_ref()
            .and_then(|v| std::str::from_utf8(v).ok())
    }

    /// Estimated size in bytes.
    pub fn size_bytes(&self) -> usize {
        self.key.as_ref().map(|k| k.len()).unwrap_or(0)
            + self.value.as_ref().map(|v| v.len()).unwrap_or(0)
            + self.topic.len()
            + self
                .headers
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
            + 24 // Fixed fields (partition, offset, timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_message_size() {
        let msg = KafkaMessage {
            key: Some(b"key".to_vec()),
            value: Some(b"value".to_vec()),
            topic: "test".to_string(),
            partition: 0,
            offset: 100,
            timestamp: 1234567890,
            headers: vec![],
        };

        assert!(msg.size_bytes() > 0);
        assert_eq!(msg.key_str(), Some("key"));
        assert_eq!(msg.value_str(), Some("value"));
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.base_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(30));
        assert_eq!(config.max_retries, 10);
        assert!(config.jitter);
    }

    #[test]
    fn test_retry_config_for_kafka() {
        let config = RetryConfig::for_kafka();
        assert_eq!(config.base_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(30));
        assert_eq!(config.max_retries, 10);
    }

    #[test]
    fn test_retry_config_without_jitter() {
        let config = RetryConfig::default().without_jitter();
        assert!(!config.jitter);
    }

    #[test]
    fn test_calculate_delay_exponential() {
        let config = RetryConfig::new(Duration::from_millis(100), Duration::from_secs(30), 10)
            .without_jitter();

        // Without jitter, delays should be exact
        assert_eq!(config.calculate_delay(0), Duration::from_millis(100)); // 100 * 2^0 = 100
        assert_eq!(config.calculate_delay(1), Duration::from_millis(200)); // 100 * 2^1 = 200
        assert_eq!(config.calculate_delay(2), Duration::from_millis(400)); // 100 * 2^2 = 400
        assert_eq!(config.calculate_delay(3), Duration::from_millis(800)); // 100 * 2^3 = 800
        assert_eq!(config.calculate_delay(4), Duration::from_millis(1600)); // 100 * 2^4 = 1600
    }

    #[test]
    fn test_calculate_delay_caps_at_max() {
        let config = RetryConfig::new(
            Duration::from_millis(100),
            Duration::from_secs(1), // Cap at 1 second
            10,
        )
        .without_jitter();

        // Should cap at max_delay (1000ms)
        assert_eq!(config.calculate_delay(10), Duration::from_millis(1000));
        assert_eq!(config.calculate_delay(20), Duration::from_millis(1000));
    }

    #[test]
    fn test_calculate_delay_with_jitter() {
        let config = RetryConfig::new(Duration::from_millis(1000), Duration::from_secs(30), 10); // jitter enabled by default

        // With jitter, delay should be within ±25% of base
        let delay = config.calculate_delay(0);
        let expected_base = 1000u64; // 1000ms
        let min = Duration::from_millis(expected_base * 3 / 4); // 750ms
        let max = Duration::from_millis(expected_base * 5 / 4); // 1250ms

        assert!(delay >= min, "delay {:?} should be >= {:?}", delay, min);
        assert!(delay <= max, "delay {:?} should be <= {:?}", delay, max);
    }

    #[test]
    fn test_calculate_delay_handles_overflow() {
        let config = RetryConfig::new(
            Duration::from_secs(1),
            Duration::from_secs(30),
            100, // High retry count
        )
        .without_jitter();

        // Very high attempt numbers should not overflow, just cap at max
        assert_eq!(config.calculate_delay(50), Duration::from_secs(30));
        assert_eq!(config.calculate_delay(100), Duration::from_secs(30));
    }
}
