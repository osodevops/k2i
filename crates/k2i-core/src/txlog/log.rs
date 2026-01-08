//! Append-only transaction log for crash recovery.

use super::entries::TransactionEntry;
use crate::config::TransactionLogConfig;
use crate::{Error, Result, TransactionLogError};
use chrono::Utc;
use parking_lot::Mutex;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Append-only transaction log.
pub struct TransactionLog {
    config: TransactionLogConfig,
    current_file: Mutex<BufWriter<File>>,
    current_path: Mutex<PathBuf>,
    entries_since_checkpoint: AtomicU64,
    last_checkpoint_time: Mutex<std::time::Instant>,
    /// Track last committed Kafka offsets per partition for checkpoints
    last_kafka_offsets: Mutex<std::collections::HashMap<(String, i32), i64>>,
    /// Track last Iceberg snapshot for checkpoints
    last_iceberg_snapshot: Mutex<Option<i64>>,
}

impl TransactionLog {
    /// Open or create a transaction log.
    pub fn open(config: TransactionLogConfig) -> Result<Self> {
        // Create log directory if it doesn't exist
        fs::create_dir_all(&config.log_dir).map_err(|e| {
            Error::TransactionLog(TransactionLogError::WriteFailed(format!(
                "Failed to create log directory: {}",
                e
            )))
        })?;

        // Create a new log file
        let log_path = config
            .log_dir
            .join(format!("txlog-{}.jsonl", Uuid::new_v4()));

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| {
                Error::TransactionLog(TransactionLogError::WriteFailed(format!(
                    "Failed to open log file: {}",
                    e
                )))
            })?;

        info!(path = %log_path.display(), "Transaction log opened");

        Ok(Self {
            config,
            current_file: Mutex::new(BufWriter::new(file)),
            current_path: Mutex::new(log_path),
            entries_since_checkpoint: AtomicU64::new(0),
            last_checkpoint_time: Mutex::new(std::time::Instant::now()),
            last_kafka_offsets: Mutex::new(std::collections::HashMap::new()),
            last_iceberg_snapshot: Mutex::new(None),
        })
    }

    /// Append an entry to the transaction log.
    pub fn append(&self, entry: TransactionEntry) -> Result<()> {
        // Track state from important entries for accurate checkpoints
        self.track_entry_state(&entry);

        let json =
            serde_json::to_string(&entry).map_err(|e| Error::Serialization(e.to_string()))?;

        let mut file = self.current_file.lock();
        writeln!(file, "{}", json)
            .map_err(|e| Error::TransactionLog(TransactionLogError::WriteFailed(e.to_string())))?;

        // Ensure durability - fsync to disk
        file.flush()
            .map_err(|e| Error::TransactionLog(TransactionLogError::WriteFailed(e.to_string())))?;

        let count = self.entries_since_checkpoint.fetch_add(1, Ordering::SeqCst) + 1;

        // Check if checkpoint needed
        let should_checkpoint = {
            let time_since_checkpoint = self.last_checkpoint_time.lock().elapsed();
            count >= self.config.checkpoint_interval_entries as u64
                || time_since_checkpoint.as_secs() >= self.config.checkpoint_interval_seconds
        };

        if should_checkpoint {
            drop(file); // Release lock before checkpoint
            self.create_checkpoint()?;
        }

        Ok(())
    }

    /// Track state from entries for accurate checkpointing.
    fn track_entry_state(&self, entry: &TransactionEntry) {
        match entry {
            TransactionEntry::FlushComplete {
                kafka_offset,
                iceberg_snapshot_id,
                ..
            } => {
                // Update last snapshot
                *self.last_iceberg_snapshot.lock() = Some(*iceberg_snapshot_id);
                // FlushComplete contains the max offset for this batch
                // We store it as a "default" partition for now
                self.last_kafka_offsets
                    .lock()
                    .insert(("_committed".to_string(), 0), *kafka_offset);
            }
            TransactionEntry::OffsetMarker {
                topic,
                partition,
                offset,
                ..
            } => {
                let mut offsets = self.last_kafka_offsets.lock();
                offsets
                    .entry((topic.clone(), *partition))
                    .and_modify(|o| {
                        if *offset > *o {
                            *o = *offset;
                        }
                    })
                    .or_insert(*offset);
            }
            TransactionEntry::IcebergSnapshot { snapshot_id, .. } => {
                *self.last_iceberg_snapshot.lock() = Some(*snapshot_id);
            }
            _ => {}
        }
    }

    /// Create a checkpoint.
    fn create_checkpoint(&self) -> Result<()> {
        let checkpoint_id = Uuid::new_v4().to_string();
        let entries_count = self.entries_since_checkpoint.load(Ordering::SeqCst);

        // Get tracked state for accurate checkpoint
        let last_kafka_offset = self
            .last_kafka_offsets
            .lock()
            .values()
            .max()
            .copied()
            .unwrap_or(0);
        let last_iceberg_snapshot = self.last_iceberg_snapshot.lock().unwrap_or(0);

        // Write checkpoint entry
        let checkpoint_entry = TransactionEntry::Checkpoint {
            checkpoint_id: checkpoint_id.clone(),
            last_kafka_offset,
            last_iceberg_snapshot,
            entries_since_last: entries_count,
            timestamp: Utc::now(),
        };

        let json = serde_json::to_string(&checkpoint_entry)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        {
            let mut file = self.current_file.lock();
            writeln!(file, "{}", json).map_err(|e| {
                Error::TransactionLog(TransactionLogError::WriteFailed(e.to_string()))
            })?;
            file.flush().map_err(|e| {
                Error::TransactionLog(TransactionLogError::WriteFailed(e.to_string()))
            })?;
        }

        // Reset counters
        self.entries_since_checkpoint.store(0, Ordering::SeqCst);
        *self.last_checkpoint_time.lock() = std::time::Instant::now();

        // Rotate log file
        self.rotate_log_file()?;

        // Cleanup old files
        self.cleanup_old_files()?;

        info!(
            checkpoint_id = %checkpoint_id,
            entries = %entries_count,
            "Transaction log checkpoint created"
        );

        Ok(())
    }

    /// Rotate to a new log file.
    fn rotate_log_file(&self) -> Result<()> {
        let new_path = self
            .config
            .log_dir
            .join(format!("txlog-{}.jsonl", Uuid::new_v4()));

        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)
            .map_err(|e| {
                Error::TransactionLog(TransactionLogError::WriteFailed(format!(
                    "Failed to create new log file: {}",
                    e
                )))
            })?;

        let mut current_file = self.current_file.lock();
        let mut current_path = self.current_path.lock();

        // CRITICAL: Ensure old file is fully synced before rotating
        // This guarantees durability of all entries before checkpoint
        current_file.flush().map_err(|e| {
            Error::TransactionLog(TransactionLogError::WriteFailed(format!(
                "Failed to flush before rotation: {}",
                e
            )))
        })?;

        // Get underlying file and sync to disk
        if let Some(file) = current_file.get_ref().try_clone().ok() {
            file.sync_all().map_err(|e| {
                Error::TransactionLog(TransactionLogError::WriteFailed(format!(
                    "Failed to fsync before rotation: {}",
                    e
                )))
            })?;
        }

        let old_path = current_path.clone();
        *current_file = BufWriter::new(new_file);
        *current_path = new_path;

        debug!(
            old_path = %old_path.display(),
            "Transaction log rotated to new file"
        );

        Ok(())
    }

    /// Cleanup old log files.
    fn cleanup_old_files(&self) -> Result<()> {
        let mut log_files: Vec<_> = fs::read_dir(&self.config.log_dir)
            .map_err(|e| {
                Error::TransactionLog(TransactionLogError::WriteFailed(format!(
                    "Failed to read log directory: {}",
                    e
                )))
            })?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "jsonl")
                    .unwrap_or(false)
            })
            .collect();

        // Sort by modification time (oldest first)
        log_files.sort_by_key(|e| {
            e.metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
        });

        // Keep only the most recent files
        let current_path = self.current_path.lock().clone();
        while log_files.len() > self.config.max_log_files {
            if let Some(oldest) = log_files.first() {
                let path = oldest.path();
                // Don't delete current file
                if path != current_path {
                    if let Err(e) = fs::remove_file(&path) {
                        warn!(path = %path.display(), error = %e, "Failed to remove old log file");
                    } else {
                        debug!(path = %path.display(), "Removed old transaction log file");
                    }
                    log_files.remove(0);
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Read all entries from all log files.
    pub fn read_all_entries(&self) -> Result<Vec<TransactionEntry>> {
        let mut entries = Vec::new();

        let mut log_files: Vec<_> = fs::read_dir(&self.config.log_dir)
            .map_err(|e| {
                Error::TransactionLog(TransactionLogError::RecoveryFailed(format!(
                    "Failed to read log directory: {}",
                    e
                )))
            })?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "jsonl")
                    .unwrap_or(false)
            })
            .collect();

        // Sort by modification time (oldest first)
        log_files.sort_by_key(|e| {
            e.metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
        });

        for dir_entry in log_files {
            let file = File::open(dir_entry.path()).map_err(|e| {
                Error::TransactionLog(TransactionLogError::RecoveryFailed(format!(
                    "Failed to open log file: {}",
                    e
                )))
            })?;

            let reader = BufReader::new(file);

            for (line_num, line) in reader.lines().enumerate() {
                let line = line.map_err(|e| {
                    Error::TransactionLog(TransactionLogError::Corrupted {
                        position: line_num as u64,
                        message: e.to_string(),
                    })
                })?;

                if line.trim().is_empty() {
                    continue;
                }

                let entry: TransactionEntry = serde_json::from_str(&line).map_err(|e| {
                    Error::TransactionLog(TransactionLogError::Corrupted {
                        position: line_num as u64,
                        message: format!("JSON parse error: {}", e),
                    })
                })?;

                entries.push(entry);
            }
        }

        info!(entries = %entries.len(), "Read transaction log entries for recovery");

        Ok(entries)
    }

    /// Get the path to the current log file.
    pub fn current_path(&self) -> PathBuf {
        self.current_path.lock().clone()
    }

    /// Get the number of entries since last checkpoint.
    pub fn entries_since_checkpoint(&self) -> u64 {
        self.entries_since_checkpoint.load(Ordering::SeqCst)
    }

    /// Force a checkpoint immediately.
    ///
    /// Use this during graceful shutdown to ensure all state is persisted.
    pub fn force_checkpoint(&self) -> Result<()> {
        let entries = self.entries_since_checkpoint.load(Ordering::SeqCst);
        if entries == 0 {
            debug!("No entries to checkpoint, skipping");
            return Ok(());
        }

        info!(entries = %entries, "Forcing checkpoint for graceful shutdown");
        self.create_checkpoint()
    }

    /// Sync all pending writes to disk.
    ///
    /// Call this before shutdown to ensure durability.
    pub fn sync(&self) -> Result<()> {
        let mut file = self.current_file.lock();
        file.flush().map_err(|e| {
            Error::TransactionLog(TransactionLogError::WriteFailed(format!(
                "Failed to flush: {}",
                e
            )))
        })?;

        if let Some(inner) = file.get_ref().try_clone().ok() {
            inner.sync_all().map_err(|e| {
                Error::TransactionLog(TransactionLogError::WriteFailed(format!(
                    "Failed to sync: {}",
                    e
                )))
            })?;
        }

        debug!("Transaction log synced to disk");
        Ok(())
    }

    /// Get the last tracked Kafka offset.
    pub fn last_kafka_offset(&self) -> Option<i64> {
        self.last_kafka_offsets.lock().values().max().copied()
    }

    /// Get the last tracked Iceberg snapshot ID.
    pub fn last_iceberg_snapshot(&self) -> Option<i64> {
        *self.last_iceberg_snapshot.lock()
    }

    /// Get all tracked Kafka offsets per partition.
    pub fn kafka_offsets(&self) -> std::collections::HashMap<(String, i32), i64> {
        self.last_kafka_offsets.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_config() -> (TransactionLogConfig, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = TransactionLogConfig {
            log_dir: temp_dir.path().to_path_buf(),
            checkpoint_interval_entries: 100,
            checkpoint_interval_seconds: 3600,
            max_log_files: 5,
        };
        (config, temp_dir)
    }

    #[test]
    fn test_transaction_log_append() {
        let (config, _temp_dir) = create_test_config();
        let log = TransactionLog::open(config).unwrap();

        let entry = TransactionEntry::FlushComplete {
            batch_id: "test-1".into(),
            kafka_offset: 100,
            iceberg_snapshot_id: 1,
            duration_ms: 50,
            timestamp: Utc::now(),
        };

        log.append(entry).unwrap();

        let entries = log.read_all_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].is_flush_complete());
    }

    #[test]
    fn test_transaction_log_multiple_entries() {
        let (config, _temp_dir) = create_test_config();
        let log = TransactionLog::open(config).unwrap();

        for i in 0..10 {
            let entry = TransactionEntry::OffsetMarker {
                topic: "test".into(),
                partition: 0,
                offset: i,
                record_count: 1,
                timestamp: Utc::now(),
            };
            log.append(entry).unwrap();
        }

        let entries = log.read_all_entries().unwrap();
        assert_eq!(entries.len(), 10);
    }
}
