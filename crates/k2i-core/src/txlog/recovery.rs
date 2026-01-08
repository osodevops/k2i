//! Crash recovery logic using transaction log.
//!
//! Provides comprehensive recovery from crashes:
//! - Detect incomplete flushes and orphan files
//! - Clean up orphaned data from object storage
//! - Restore Kafka consumer offsets
//! - Resume from last consistent state

use super::entries::{MaintenanceOp, TransactionEntry};
use super::log::TransactionLog;
use crate::Result;
use object_store::ObjectStore;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// State recovered from the transaction log.
#[derive(Debug, Default)]
pub struct RecoveryState {
    /// Last successfully committed Kafka offset per partition
    pub last_kafka_offsets: HashMap<(String, i32), i64>,

    /// Last Iceberg snapshot ID
    pub last_iceberg_snapshot: Option<i64>,

    /// Incomplete flush batch IDs (started but not completed)
    pub incomplete_flushes: Vec<String>,

    /// Orphan Parquet files (written but never committed to Iceberg)
    pub orphan_files: Vec<OrphanFile>,

    /// Successfully committed file paths (for verification)
    pub committed_files: HashSet<String>,

    /// Total entries processed
    pub entries_processed: u64,
}

/// Information about an orphan file that needs cleanup.
#[derive(Debug, Clone)]
pub struct OrphanFile {
    /// Batch ID that created this file
    pub batch_id: String,
    /// File path in object storage
    pub file_path: String,
    /// File size in bytes
    pub file_size_bytes: u64,
    /// Reason the file is orphaned
    pub reason: OrphanReason,
}

/// Why a file became orphaned.
#[derive(Debug, Clone, PartialEq)]
pub enum OrphanReason {
    /// Flush started but never completed (crash mid-write)
    IncompleteFlush,
    /// Parquet written but Iceberg commit failed
    CommitFailed,
}

impl RecoveryState {
    /// Recover state from a transaction log.
    pub fn recover_from(txlog: &TransactionLog) -> Result<Self> {
        info!("Starting recovery from transaction log");

        let entries = txlog.read_all_entries()?;
        let mut state = RecoveryState::default();

        // Track flush states and file writes
        let mut flush_started: HashMap<String, bool> = HashMap::new();
        let mut flush_completed: HashMap<String, bool> = HashMap::new();
        let mut parquet_written: HashMap<String, (String, u64)> = HashMap::new(); // batch_id -> (path, size)
        let mut iceberg_committed: HashSet<String> = HashSet::new(); // batch_ids that got Iceberg snapshot

        for entry in entries {
            state.entries_processed += 1;

            match entry {
                TransactionEntry::FlushStart { batch_id, .. } => {
                    flush_started.insert(batch_id, true);
                }

                TransactionEntry::ParquetWritten {
                    batch_id,
                    file_path,
                    file_size_bytes,
                    ..
                } => {
                    parquet_written.insert(batch_id, (file_path, file_size_bytes));
                }

                TransactionEntry::IcebergSnapshot { batch_id, .. } => {
                    iceberg_committed.insert(batch_id);
                }

                TransactionEntry::FlushComplete {
                    batch_id,
                    kafka_offset,
                    iceberg_snapshot_id,
                    ..
                } => {
                    flush_completed.insert(batch_id.clone(), true);
                    state.last_iceberg_snapshot = Some(iceberg_snapshot_id);

                    // Track committed file
                    if let Some((path, _)) = parquet_written.get(&batch_id) {
                        state.committed_files.insert(path.clone());
                    }

                    // We don't have partition info here, so we use a default
                    // In production, we'd track per-partition offsets
                    state
                        .last_kafka_offsets
                        .insert(("default".to_string(), 0), kafka_offset);
                }

                TransactionEntry::OffsetMarker {
                    topic,
                    partition,
                    offset,
                    ..
                } => {
                    let key = (topic, partition);
                    state
                        .last_kafka_offsets
                        .entry(key)
                        .and_modify(|o| {
                            if offset > *o {
                                *o = offset;
                            }
                        })
                        .or_insert(offset);
                }

                TransactionEntry::Checkpoint {
                    last_kafka_offset,
                    last_iceberg_snapshot,
                    ..
                } => {
                    if last_iceberg_snapshot > 0 {
                        state.last_iceberg_snapshot = Some(last_iceberg_snapshot);
                    }
                    if last_kafka_offset > 0 {
                        state
                            .last_kafka_offsets
                            .insert(("default".to_string(), 0), last_kafka_offset);
                    }
                }

                _ => {}
            }
        }

        // Find incomplete flushes and orphan files
        for (batch_id, _) in flush_started.iter() {
            if !flush_completed.contains_key(batch_id) {
                state.incomplete_flushes.push(batch_id.clone());

                // Check if this batch wrote a Parquet file that's now orphaned
                if let Some((file_path, file_size_bytes)) = parquet_written.get(batch_id) {
                    let reason = if iceberg_committed.contains(batch_id) {
                        // Iceberg commit succeeded but flush didn't complete
                        // This is actually not an orphan - Iceberg has it
                        continue;
                    } else {
                        OrphanReason::IncompleteFlush
                    };

                    state.orphan_files.push(OrphanFile {
                        batch_id: batch_id.clone(),
                        file_path: file_path.clone(),
                        file_size_bytes: *file_size_bytes,
                        reason,
                    });

                    warn!(
                        batch_id = %batch_id,
                        file_path = %file_path,
                        "Found orphan file from incomplete flush"
                    );
                }
            }
        }

        info!(
            entries = %state.entries_processed,
            last_snapshot = ?state.last_iceberg_snapshot,
            incomplete_flushes = %state.incomplete_flushes.len(),
            orphan_files = %state.orphan_files.len(),
            "Recovery complete"
        );

        Ok(state)
    }

    /// Check if there are incomplete operations that need cleanup.
    pub fn needs_cleanup(&self) -> bool {
        !self.incomplete_flushes.is_empty() || !self.orphan_files.is_empty()
    }

    /// Check if there are orphan files to clean up.
    pub fn has_orphan_files(&self) -> bool {
        !self.orphan_files.is_empty()
    }

    /// Get the total size of orphan files in bytes.
    pub fn orphan_files_size(&self) -> u64 {
        self.orphan_files.iter().map(|f| f.file_size_bytes).sum()
    }

    /// Get the max Kafka offset across all partitions.
    pub fn max_kafka_offset(&self) -> Option<i64> {
        self.last_kafka_offsets.values().max().copied()
    }

    /// Get a summary of the recovery state for logging.
    pub fn summary(&self) -> RecoverySummary {
        RecoverySummary {
            entries_processed: self.entries_processed,
            last_iceberg_snapshot: self.last_iceberg_snapshot,
            max_kafka_offset: self.max_kafka_offset(),
            partition_count: self.last_kafka_offsets.len(),
            incomplete_flush_count: self.incomplete_flushes.len(),
            orphan_file_count: self.orphan_files.len(),
            orphan_file_size_bytes: self.orphan_files_size(),
            committed_file_count: self.committed_files.len(),
        }
    }

    /// Log cleanup actions to the transaction log.
    pub fn log_cleanup_actions(&self, txlog: &TransactionLog) -> Result<()> {
        use chrono::Utc;

        for orphan in &self.orphan_files {
            debug!(
                batch_id = %orphan.batch_id,
                file_path = %orphan.file_path,
                reason = ?orphan.reason,
                "Logging orphan file cleanup"
            );

            txlog.append(TransactionEntry::Maintenance {
                operation: MaintenanceOp::OrphanCleaned,
                details: format!(
                    "Cleaned orphan file {} from batch {} (reason: {:?})",
                    orphan.file_path, orphan.batch_id, orphan.reason
                ),
                timestamp: Utc::now(),
            })?;
        }

        Ok(())
    }

    /// Execute cleanup of orphan files from object storage.
    ///
    /// This method actually deletes the orphan files from storage.
    /// Call `log_cleanup_actions` before or after to record the cleanup.
    pub async fn execute_cleanup(
        &self,
        object_store: &Arc<dyn ObjectStore>,
        txlog: &TransactionLog,
    ) -> Result<CleanupResult> {
        use chrono::Utc;

        if self.orphan_files.is_empty() {
            debug!("No orphan files to clean up");
            return Ok(CleanupResult::default());
        }

        info!(
            orphan_count = self.orphan_files.len(),
            total_size_bytes = self.orphan_files_size(),
            "Starting orphan file cleanup"
        );

        let mut files_deleted = 0;
        let mut bytes_freed = 0u64;
        let mut failed_deletions = Vec::new();

        for orphan in &self.orphan_files {
            let path = object_store::path::Path::from(orphan.file_path.as_str());

            match object_store.delete(&path).await {
                Ok(()) => {
                    files_deleted += 1;
                    bytes_freed += orphan.file_size_bytes;

                    debug!(
                        batch_id = %orphan.batch_id,
                        file_path = %orphan.file_path,
                        size_bytes = orphan.file_size_bytes,
                        "Deleted orphan file"
                    );

                    // Log to txlog
                    txlog.append(TransactionEntry::Maintenance {
                        operation: MaintenanceOp::OrphanCleaned,
                        details: format!(
                            "Deleted orphan file {} ({} bytes) from batch {} (reason: {:?})",
                            orphan.file_path,
                            orphan.file_size_bytes,
                            orphan.batch_id,
                            orphan.reason
                        ),
                        timestamp: Utc::now(),
                    })?;
                }
                Err(e) => {
                    warn!(
                        batch_id = %orphan.batch_id,
                        file_path = %orphan.file_path,
                        error = %e,
                        "Failed to delete orphan file"
                    );
                    failed_deletions.push((orphan.file_path.clone(), e.to_string()));
                }
            }
        }

        info!(
            files_deleted = files_deleted,
            bytes_freed = bytes_freed,
            failed_count = failed_deletions.len(),
            "Orphan file cleanup complete"
        );

        Ok(CleanupResult {
            files_deleted,
            bytes_freed,
            failed_deletions,
        })
    }

    /// Get the starting Kafka offset for a specific partition after recovery.
    ///
    /// Returns the next offset to consume (last committed + 1).
    pub fn starting_offset_for(&self, topic: &str, partition: i32) -> Option<i64> {
        self.last_kafka_offsets
            .get(&(topic.to_string(), partition))
            .map(|offset| offset + 1)
    }

    /// Check if we should skip messages up to a certain offset based on recovery state.
    ///
    /// Returns true if the message has already been processed.
    pub fn is_already_processed(&self, topic: &str, partition: i32, offset: i64) -> bool {
        self.last_kafka_offsets
            .get(&(topic.to_string(), partition))
            .map(|last_offset| offset <= *last_offset)
            .unwrap_or(false)
    }
}

/// Result of cleanup operation.
#[derive(Debug, Clone, Default)]
pub struct CleanupResult {
    /// Number of orphan files successfully deleted
    pub files_deleted: usize,
    /// Total bytes freed by deletion
    pub bytes_freed: u64,
    /// Files that failed to delete with error messages
    pub failed_deletions: Vec<(String, String)>,
}

impl CleanupResult {
    /// Check if cleanup was fully successful.
    pub fn is_complete(&self) -> bool {
        self.failed_deletions.is_empty()
    }

    /// Get the number of failed deletions.
    pub fn failed_count(&self) -> usize {
        self.failed_deletions.len()
    }
}

/// Summary of recovery state for logging/reporting.
#[derive(Debug, Clone)]
pub struct RecoverySummary {
    /// Number of log entries processed
    pub entries_processed: u64,
    /// Last Iceberg snapshot ID
    pub last_iceberg_snapshot: Option<i64>,
    /// Maximum Kafka offset seen
    pub max_kafka_offset: Option<i64>,
    /// Number of partitions tracked
    pub partition_count: usize,
    /// Number of incomplete flushes
    pub incomplete_flush_count: usize,
    /// Number of orphan files
    pub orphan_file_count: usize,
    /// Total size of orphan files
    pub orphan_file_size_bytes: u64,
    /// Number of successfully committed files
    pub committed_file_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TransactionLogConfig;
    use chrono::Utc;
    use tempfile::TempDir;

    fn create_test_log() -> (TransactionLog, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = TransactionLogConfig {
            log_dir: temp_dir.path().to_path_buf(),
            checkpoint_interval_entries: 1000,
            checkpoint_interval_seconds: 3600,
            max_log_files: 5,
        };
        let log = TransactionLog::open(config).unwrap();
        (log, temp_dir)
    }

    #[test]
    fn test_recovery_empty_log() {
        let (log, _temp_dir) = create_test_log();
        let state = RecoveryState::recover_from(&log).unwrap();

        assert_eq!(state.entries_processed, 0);
        assert!(state.last_iceberg_snapshot.is_none());
        assert!(state.incomplete_flushes.is_empty());
    }

    #[test]
    fn test_recovery_with_complete_flush() {
        let (log, _temp_dir) = create_test_log();

        log.append(TransactionEntry::FlushStart {
            batch_id: "batch-1".into(),
            row_count: 100,
            timestamp: Utc::now(),
        })
        .unwrap();

        log.append(TransactionEntry::FlushComplete {
            batch_id: "batch-1".into(),
            kafka_offset: 500,
            iceberg_snapshot_id: 42,
            duration_ms: 100,
            timestamp: Utc::now(),
        })
        .unwrap();

        let state = RecoveryState::recover_from(&log).unwrap();

        assert_eq!(state.entries_processed, 2);
        assert_eq!(state.last_iceberg_snapshot, Some(42));
        assert!(state.incomplete_flushes.is_empty());
    }

    #[test]
    fn test_recovery_with_incomplete_flush() {
        let (log, _temp_dir) = create_test_log();

        log.append(TransactionEntry::FlushStart {
            batch_id: "batch-incomplete".into(),
            row_count: 100,
            timestamp: Utc::now(),
        })
        .unwrap();

        // No FlushComplete entry

        let state = RecoveryState::recover_from(&log).unwrap();

        assert!(state.needs_cleanup());
        assert_eq!(state.incomplete_flushes, vec!["batch-incomplete"]);
    }

    #[test]
    fn test_recovery_orphan_file_detection() {
        let (log, _temp_dir) = create_test_log();

        // Start a flush
        log.append(TransactionEntry::FlushStart {
            batch_id: "batch-orphan".into(),
            row_count: 100,
            timestamp: Utc::now(),
        })
        .unwrap();

        // Write Parquet file but never commit to Iceberg
        log.append(TransactionEntry::ParquetWritten {
            batch_id: "batch-orphan".into(),
            file_path: "/data/orphan-file.parquet".into(),
            file_size_bytes: 1024,
            row_count: 100,
            checksum: "abc123".into(),
            timestamp: Utc::now(),
        })
        .unwrap();

        // No IcebergSnapshot or FlushComplete - simulates crash

        let state = RecoveryState::recover_from(&log).unwrap();

        assert!(state.has_orphan_files());
        assert_eq!(state.orphan_files.len(), 1);
        assert_eq!(state.orphan_files[0].file_path, "/data/orphan-file.parquet");
        assert_eq!(state.orphan_files[0].file_size_bytes, 1024);
        assert_eq!(state.orphan_files[0].reason, OrphanReason::IncompleteFlush);
        assert_eq!(state.orphan_files_size(), 1024);
    }

    #[test]
    fn test_recovery_committed_file_tracking() {
        let (log, _temp_dir) = create_test_log();

        log.append(TransactionEntry::FlushStart {
            batch_id: "batch-1".into(),
            row_count: 100,
            timestamp: Utc::now(),
        })
        .unwrap();

        log.append(TransactionEntry::ParquetWritten {
            batch_id: "batch-1".into(),
            file_path: "/data/committed-file.parquet".into(),
            file_size_bytes: 2048,
            row_count: 100,
            checksum: "def456".into(),
            timestamp: Utc::now(),
        })
        .unwrap();

        log.append(TransactionEntry::IcebergSnapshot {
            batch_id: "batch-1".into(),
            snapshot_id: 123,
            manifest_list_path: "/metadata/snap-123.avro".into(),
            row_count_total: 100,
            timestamp: Utc::now(),
        })
        .unwrap();

        log.append(TransactionEntry::FlushComplete {
            batch_id: "batch-1".into(),
            kafka_offset: 1000,
            iceberg_snapshot_id: 123,
            duration_ms: 50,
            timestamp: Utc::now(),
        })
        .unwrap();

        let state = RecoveryState::recover_from(&log).unwrap();

        assert!(!state.has_orphan_files());
        assert!(state
            .committed_files
            .contains("/data/committed-file.parquet"));
        assert_eq!(state.last_iceberg_snapshot, Some(123));
    }

    #[test]
    fn test_recovery_multiple_partitions() {
        let (log, _temp_dir) = create_test_log();

        // Track offsets from multiple partitions
        log.append(TransactionEntry::OffsetMarker {
            topic: "events".into(),
            partition: 0,
            offset: 100,
            record_count: 10,
            timestamp: Utc::now(),
        })
        .unwrap();

        log.append(TransactionEntry::OffsetMarker {
            topic: "events".into(),
            partition: 1,
            offset: 200,
            record_count: 10,
            timestamp: Utc::now(),
        })
        .unwrap();

        log.append(TransactionEntry::OffsetMarker {
            topic: "events".into(),
            partition: 0,
            offset: 150, // Higher offset for partition 0
            record_count: 10,
            timestamp: Utc::now(),
        })
        .unwrap();

        let state = RecoveryState::recover_from(&log).unwrap();

        assert_eq!(
            state.last_kafka_offsets.get(&("events".to_string(), 0)),
            Some(&150)
        );
        assert_eq!(
            state.last_kafka_offsets.get(&("events".to_string(), 1)),
            Some(&200)
        );
        assert_eq!(state.max_kafka_offset(), Some(200));
    }

    #[test]
    fn test_recovery_summary() {
        let (log, _temp_dir) = create_test_log();

        log.append(TransactionEntry::FlushStart {
            batch_id: "batch-1".into(),
            row_count: 100,
            timestamp: Utc::now(),
        })
        .unwrap();

        log.append(TransactionEntry::FlushComplete {
            batch_id: "batch-1".into(),
            kafka_offset: 500,
            iceberg_snapshot_id: 42,
            duration_ms: 100,
            timestamp: Utc::now(),
        })
        .unwrap();

        let state = RecoveryState::recover_from(&log).unwrap();
        let summary = state.summary();

        assert_eq!(summary.entries_processed, 2);
        assert_eq!(summary.last_iceberg_snapshot, Some(42));
        assert_eq!(summary.incomplete_flush_count, 0);
        assert_eq!(summary.orphan_file_count, 0);
    }

    #[test]
    fn test_checkpoint_tracks_state() {
        let (log, _temp_dir) = create_test_log();

        // Append some entries that should update checkpoint state
        log.append(TransactionEntry::FlushComplete {
            batch_id: "batch-1".into(),
            kafka_offset: 1000,
            iceberg_snapshot_id: 99,
            duration_ms: 50,
            timestamp: Utc::now(),
        })
        .unwrap();

        // Check that the log tracked the state
        assert_eq!(log.last_kafka_offset(), Some(1000));
        assert_eq!(log.last_iceberg_snapshot(), Some(99));
    }

    #[test]
    fn test_force_checkpoint() {
        let (log, _temp_dir) = create_test_log();

        // Force checkpoint with no entries should succeed
        log.force_checkpoint().unwrap();

        // Add some entries
        log.append(TransactionEntry::FlushComplete {
            batch_id: "batch-1".into(),
            kafka_offset: 500,
            iceberg_snapshot_id: 42,
            duration_ms: 100,
            timestamp: Utc::now(),
        })
        .unwrap();

        // Force checkpoint should now create a checkpoint
        log.force_checkpoint().unwrap();

        // Read all entries - should have original + checkpoint
        let entries = log.read_all_entries().unwrap();
        let checkpoint_count = entries.iter().filter(|e| e.is_checkpoint()).count();
        assert!(checkpoint_count >= 1);
    }
}
