//! Offset tracking for Kafka partitions.

use dashmap::DashMap;
use std::sync::atomic::{AtomicI64, Ordering};

/// Tracks offsets for Kafka partitions.
pub struct OffsetTracker {
    /// Current offset per partition (topic, partition) -> offset
    current_offsets: DashMap<(String, i32), AtomicI64>,

    /// Last committed offset per partition
    committed_offsets: DashMap<(String, i32), AtomicI64>,
}

impl OffsetTracker {
    /// Create a new offset tracker.
    pub fn new() -> Self {
        Self {
            current_offsets: DashMap::new(),
            committed_offsets: DashMap::new(),
        }
    }

    /// Update the current offset for a partition.
    pub fn update_current(&self, topic: &str, partition: i32, offset: i64) {
        let key = (topic.to_string(), partition);
        self.current_offsets
            .entry(key)
            .and_modify(|v| v.store(offset, Ordering::SeqCst))
            .or_insert_with(|| AtomicI64::new(offset));
    }

    /// Get the current offset for a partition.
    pub fn get_current(&self, topic: &str, partition: i32) -> Option<i64> {
        let key = (topic.to_string(), partition);
        self.current_offsets
            .get(&key)
            .map(|v| v.load(Ordering::SeqCst))
    }

    /// Mark an offset as committed.
    pub fn mark_committed(&self, topic: &str, partition: i32, offset: i64) {
        let key = (topic.to_string(), partition);
        self.committed_offsets
            .entry(key)
            .and_modify(|v| v.store(offset, Ordering::SeqCst))
            .or_insert_with(|| AtomicI64::new(offset));
    }

    /// Get the committed offset for a partition.
    pub fn get_committed(&self, topic: &str, partition: i32) -> Option<i64> {
        let key = (topic.to_string(), partition);
        self.committed_offsets
            .get(&key)
            .map(|v| v.load(Ordering::SeqCst))
    }

    /// Get the lag (difference between current and committed) for a partition.
    pub fn get_lag(&self, topic: &str, partition: i32) -> Option<i64> {
        let current = self.get_current(topic, partition)?;
        let committed = self.get_committed(topic, partition).unwrap_or(0);
        Some(current - committed)
    }

    /// Get all tracked partitions.
    pub fn get_all_partitions(&self) -> Vec<(String, i32)> {
        self.current_offsets
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get the maximum offset across all partitions.
    pub fn max_offset(&self) -> Option<i64> {
        self.current_offsets
            .iter()
            .map(|entry| entry.value().load(Ordering::SeqCst))
            .max()
    }

    /// Clear all tracked offsets.
    pub fn clear(&self) {
        self.current_offsets.clear();
        self.committed_offsets.clear();
    }
}

impl Default for OffsetTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_tracker() {
        let tracker = OffsetTracker::new();

        tracker.update_current("test", 0, 100);
        assert_eq!(tracker.get_current("test", 0), Some(100));

        tracker.mark_committed("test", 0, 50);
        assert_eq!(tracker.get_committed("test", 0), Some(50));
        assert_eq!(tracker.get_lag("test", 0), Some(50));

        tracker.update_current("test", 0, 200);
        assert_eq!(tracker.get_lag("test", 0), Some(150));
    }

    #[test]
    fn test_offset_tracker_multiple_partitions() {
        let tracker = OffsetTracker::new();

        tracker.update_current("test", 0, 100);
        tracker.update_current("test", 1, 200);
        tracker.update_current("other", 0, 50);

        let partitions = tracker.get_all_partitions();
        assert_eq!(partitions.len(), 3);

        assert_eq!(tracker.max_offset(), Some(200));
    }
}
