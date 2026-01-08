//! Eviction policies for the hot buffer.
//!
//! Supports TTL-based and size-based eviction strategies.

use std::time::{Duration, Instant};

/// Eviction policy configuration.
#[derive(Debug, Clone)]
pub struct EvictionPolicy {
    /// TTL for records (evict after this duration)
    pub ttl: Duration,

    /// Maximum buffer size in bytes
    pub max_size_bytes: usize,

    /// Maximum number of records
    pub max_records: usize,

    /// Flush batch size threshold
    pub flush_batch_size: usize,

    /// Flush interval
    pub flush_interval: Duration,
}

impl EvictionPolicy {
    /// Create a new eviction policy with defaults.
    pub fn new() -> Self {
        Self {
            ttl: Duration::from_secs(60),
            max_size_bytes: 500 * 1024 * 1024, // 500MB
            max_records: 100_000,
            flush_batch_size: 10_000,
            flush_interval: Duration::from_secs(30),
        }
    }

    /// Set TTL.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Set max size in bytes.
    pub fn with_max_size_bytes(mut self, size: usize) -> Self {
        self.max_size_bytes = size;
        self
    }

    /// Set max records.
    pub fn with_max_records(mut self, count: usize) -> Self {
        self.max_records = count;
        self
    }

    /// Set flush batch size.
    pub fn with_flush_batch_size(mut self, size: usize) -> Self {
        self.flush_batch_size = size;
        self
    }

    /// Set flush interval.
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        Self::new()
    }
}

/// Eviction decision.
#[derive(Debug, Clone, PartialEq)]
pub enum EvictionDecision {
    /// No eviction needed
    None,
    /// Flush due to TTL expiration
    FlushTtl,
    /// Flush due to size limit
    FlushSize,
    /// Flush due to record count
    FlushCount,
    /// Flush due to time interval
    FlushInterval,
    /// Buffer is full, must evict immediately
    EvictImmediate,
}

impl EvictionDecision {
    /// Check if this decision requires a flush.
    pub fn requires_flush(&self) -> bool {
        !matches!(self, EvictionDecision::None)
    }

    /// Check if this is an immediate eviction (backpressure).
    pub fn is_immediate(&self) -> bool {
        matches!(self, EvictionDecision::EvictImmediate)
    }
}

/// Eviction checker that evaluates buffer state against policy.
pub struct EvictionChecker {
    policy: EvictionPolicy,
    created_at: Instant,
    last_flush: Instant,
}

impl EvictionChecker {
    /// Create a new eviction checker.
    pub fn new(policy: EvictionPolicy) -> Self {
        let now = Instant::now();
        Self {
            policy,
            created_at: now,
            last_flush: now,
        }
    }

    /// Check if eviction is needed based on current buffer state.
    pub fn check(&self, current_size_bytes: usize, record_count: usize) -> EvictionDecision {
        // Check immediate eviction (buffer full)
        if current_size_bytes >= self.policy.max_size_bytes {
            return EvictionDecision::EvictImmediate;
        }

        // Check record count limit
        if record_count >= self.policy.max_records {
            return EvictionDecision::FlushCount;
        }

        // Check flush batch size
        if record_count >= self.policy.flush_batch_size {
            return EvictionDecision::FlushCount;
        }

        // Check TTL (buffer age)
        if self.created_at.elapsed() >= self.policy.ttl && record_count > 0 {
            return EvictionDecision::FlushTtl;
        }

        // Check flush interval
        if self.last_flush.elapsed() >= self.policy.flush_interval && record_count > 0 {
            return EvictionDecision::FlushInterval;
        }

        EvictionDecision::None
    }

    /// Mark that a flush occurred.
    pub fn mark_flushed(&mut self) {
        self.last_flush = Instant::now();
    }

    /// Get time since last flush.
    pub fn time_since_flush(&self) -> Duration {
        self.last_flush.elapsed()
    }

    /// Get buffer age.
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Get the policy.
    pub fn policy(&self) -> &EvictionPolicy {
        &self.policy
    }
}

/// Record metadata for TTL tracking.
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    /// When the record was inserted
    pub inserted_at: Instant,

    /// Kafka partition
    pub partition: i32,

    /// Kafka offset
    pub offset: i64,

    /// Record size in bytes
    pub size_bytes: usize,
}

impl RecordMetadata {
    /// Create new record metadata.
    pub fn new(partition: i32, offset: i64, size_bytes: usize) -> Self {
        Self {
            inserted_at: Instant::now(),
            partition,
            offset,
            size_bytes,
        }
    }

    /// Check if this record has expired based on TTL.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.inserted_at.elapsed() >= ttl
    }

    /// Get record age.
    pub fn age(&self) -> Duration {
        self.inserted_at.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eviction_policy_defaults() {
        let policy = EvictionPolicy::new();
        assert_eq!(policy.ttl, Duration::from_secs(60));
        assert_eq!(policy.max_size_bytes, 500 * 1024 * 1024);
    }

    #[test]
    fn test_eviction_policy_builder() {
        let policy = EvictionPolicy::new()
            .with_ttl(Duration::from_secs(30))
            .with_max_size_bytes(100 * 1024 * 1024)
            .with_flush_batch_size(5000);

        assert_eq!(policy.ttl, Duration::from_secs(30));
        assert_eq!(policy.max_size_bytes, 100 * 1024 * 1024);
        assert_eq!(policy.flush_batch_size, 5000);
    }

    #[test]
    fn test_eviction_checker_size_limit() {
        let policy = EvictionPolicy::new().with_max_size_bytes(1000);
        let checker = EvictionChecker::new(policy);

        // Under limit
        let decision = checker.check(500, 10);
        assert_eq!(decision, EvictionDecision::None);

        // At limit
        let decision = checker.check(1000, 10);
        assert_eq!(decision, EvictionDecision::EvictImmediate);

        // Over limit
        let decision = checker.check(1500, 10);
        assert_eq!(decision, EvictionDecision::EvictImmediate);
    }

    #[test]
    fn test_eviction_checker_record_count() {
        let policy = EvictionPolicy::new()
            .with_max_size_bytes(1_000_000)
            .with_flush_batch_size(100);
        let checker = EvictionChecker::new(policy);

        // Under limit
        let decision = checker.check(500, 50);
        assert_eq!(decision, EvictionDecision::None);

        // At batch size
        let decision = checker.check(500, 100);
        assert_eq!(decision, EvictionDecision::FlushCount);
    }

    #[test]
    fn test_eviction_decision_requires_flush() {
        assert!(!EvictionDecision::None.requires_flush());
        assert!(EvictionDecision::FlushTtl.requires_flush());
        assert!(EvictionDecision::FlushSize.requires_flush());
        assert!(EvictionDecision::FlushCount.requires_flush());
        assert!(EvictionDecision::FlushInterval.requires_flush());
        assert!(EvictionDecision::EvictImmediate.requires_flush());
    }

    #[test]
    fn test_eviction_decision_is_immediate() {
        assert!(!EvictionDecision::None.is_immediate());
        assert!(!EvictionDecision::FlushTtl.is_immediate());
        assert!(EvictionDecision::EvictImmediate.is_immediate());
    }

    #[test]
    fn test_record_metadata() {
        let meta = RecordMetadata::new(0, 100, 1024);
        assert_eq!(meta.partition, 0);
        assert_eq!(meta.offset, 100);
        assert_eq!(meta.size_bytes, 1024);
        assert!(!meta.is_expired(Duration::from_secs(60)));
    }

    #[test]
    fn test_eviction_checker_mark_flushed() {
        let policy = EvictionPolicy::new();
        let mut checker = EvictionChecker::new(policy);

        // Wait to accumulate some time since last flush
        std::thread::sleep(Duration::from_millis(20));
        let before = checker.time_since_flush();
        assert!(before >= Duration::from_millis(15)); // Should be at least ~20ms

        // Mark as flushed - this resets the timer
        checker.mark_flushed();
        let after = checker.time_since_flush();

        // After marking flushed, time_since_flush should be very small (near zero)
        assert!(after < Duration::from_millis(10));
        assert!(after < before);
    }
}
