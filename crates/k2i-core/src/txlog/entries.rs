//! Transaction log entry types.
//!
//! Each entry type represents a step in the ingestion pipeline
//! and is used for crash recovery.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A transaction log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TransactionEntry {
    /// Offset position marker (recorded before Kafka commit)
    OffsetMarker {
        /// Topic name
        topic: String,
        /// Partition number
        partition: i32,
        /// Offset value
        offset: i64,
        /// Number of records at this offset
        record_count: u32,
        /// Timestamp
        timestamp: DateTime<Utc>,
    },

    /// Buffer snapshot recorded
    BufferSnapshot {
        /// Unique batch identifier
        batch_id: String,
        /// Number of rows in the snapshot
        row_count: u32,
        /// Memory size in bytes
        memory_bytes: u64,
        /// Minimum Kafka offset in this batch
        min_offset: i64,
        /// Maximum Kafka offset in this batch
        max_offset: i64,
        /// Timestamp
        timestamp: DateTime<Utc>,
    },

    /// Flush operation started
    FlushStart {
        /// Unique batch identifier
        batch_id: String,
        /// Number of rows to flush
        row_count: u32,
        /// Timestamp
        timestamp: DateTime<Utc>,
    },

    /// Parquet file written to storage
    ParquetWritten {
        /// Unique batch identifier
        batch_id: String,
        /// File path in object storage
        file_path: String,
        /// File size in bytes
        file_size_bytes: u64,
        /// Number of rows in the file
        row_count: u32,
        /// CRC32 checksum of the file
        checksum: String,
        /// Timestamp
        timestamp: DateTime<Utc>,
    },

    /// Iceberg snapshot created
    IcebergSnapshot {
        /// Unique batch identifier
        batch_id: String,
        /// Iceberg snapshot ID
        snapshot_id: i64,
        /// Manifest list path
        manifest_list_path: String,
        /// Total row count in the table
        row_count_total: u64,
        /// Timestamp
        timestamp: DateTime<Utc>,
    },

    /// Flush completed successfully
    ///
    /// CRITICAL: Only after this entry can we commit Kafka offsets!
    FlushComplete {
        /// Unique batch identifier
        batch_id: String,
        /// Kafka offset that was flushed
        kafka_offset: i64,
        /// Iceberg snapshot ID
        iceberg_snapshot_id: i64,
        /// Duration in milliseconds
        duration_ms: u64,
        /// Timestamp
        timestamp: DateTime<Utc>,
    },

    /// Checkpoint marker
    Checkpoint {
        /// Checkpoint identifier
        checkpoint_id: String,
        /// Last Kafka offset at checkpoint
        last_kafka_offset: i64,
        /// Last Iceberg snapshot at checkpoint
        last_iceberg_snapshot: i64,
        /// Number of entries since last checkpoint
        entries_since_last: u64,
        /// Timestamp
        timestamp: DateTime<Utc>,
    },

    /// Maintenance operation
    Maintenance {
        /// Type of maintenance operation
        operation: MaintenanceOp,
        /// Details about the operation
        details: String,
        /// Timestamp
        timestamp: DateTime<Utc>,
    },

    /// Catalog health check result
    ///
    /// Records periodic health check results for monitoring and alerting.
    CatalogHealthCheck {
        /// Type of catalog (REST, Hive, Glue, Nessie)
        catalog_type: String,
        /// Whether the catalog is healthy
        is_healthy: bool,
        /// Response time in milliseconds
        response_time_ms: u64,
        /// Additional health details
        details: Option<String>,
        /// Timestamp
        timestamp: DateTime<Utc>,
    },

    /// Catalog error event
    ///
    /// Records catalog failures for debugging and alerting.
    CatalogError {
        /// Type of catalog (REST, Hive, Glue, Nessie)
        catalog_type: String,
        /// Error message
        error_message: String,
        /// Error code (if available)
        error_code: Option<String>,
        /// Retry count before this error
        retry_count: u32,
        /// Whether the error is retryable
        is_retryable: bool,
        /// Timestamp
        timestamp: DateTime<Utc>,
    },

    /// Idempotency record for deduplication
    ///
    /// CRITICAL: Used to ensure exactly-once semantics by tracking
    /// which Kafka offset ranges have already been committed to Iceberg.
    IdempotencyRecord {
        /// Minimum Kafka offset in the committed range
        kafka_offset_min: i64,
        /// Maximum Kafka offset in the committed range
        kafka_offset_max: i64,
        /// Topic name
        topic: String,
        /// Partition number
        partition: i32,
        /// Iceberg snapshot ID for this commit
        snapshot_id: i64,
        /// File paths included in this commit
        file_paths: Vec<String>,
        /// Timestamp when committed
        committed_at: DateTime<Utc>,
    },

    /// Schema evolution event
    ///
    /// Records schema changes for auditing and debugging.
    SchemaEvolution {
        /// Database name
        database: String,
        /// Table name
        table: String,
        /// Fields added in this evolution
        fields_added: Vec<SchemaField>,
        /// Fields removed in this evolution (soft delete)
        fields_removed: Vec<String>,
        /// Old schema version/ID
        old_schema_id: Option<i32>,
        /// New schema version/ID
        new_schema_id: i32,
        /// Reason for evolution (auto, manual, etc.)
        reason: String,
        /// Timestamp
        timestamp: DateTime<Utc>,
    },
}

/// Schema field definition for schema evolution tracking.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaField {
    /// Field name
    pub name: String,
    /// Field type (as string, e.g., "string", "long", "timestamp")
    pub field_type: String,
    /// Whether the field is required
    pub required: bool,
    /// Optional documentation
    pub doc: Option<String>,
}

/// Types of maintenance operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceOp {
    /// Compaction started
    CompactionStarted,
    /// Compaction completed
    CompactionCompleted,
    /// Snapshot expired
    SnapshotExpired,
    /// Orphan file cleaned
    OrphanCleaned,
    /// Statistics updated
    StatisticsUpdated,
}

impl TransactionEntry {
    /// Get the timestamp of this entry.
    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            TransactionEntry::OffsetMarker { timestamp, .. } => *timestamp,
            TransactionEntry::BufferSnapshot { timestamp, .. } => *timestamp,
            TransactionEntry::FlushStart { timestamp, .. } => *timestamp,
            TransactionEntry::ParquetWritten { timestamp, .. } => *timestamp,
            TransactionEntry::IcebergSnapshot { timestamp, .. } => *timestamp,
            TransactionEntry::FlushComplete { timestamp, .. } => *timestamp,
            TransactionEntry::Checkpoint { timestamp, .. } => *timestamp,
            TransactionEntry::Maintenance { timestamp, .. } => *timestamp,
            TransactionEntry::CatalogHealthCheck { timestamp, .. } => *timestamp,
            TransactionEntry::CatalogError { timestamp, .. } => *timestamp,
            TransactionEntry::IdempotencyRecord { committed_at, .. } => *committed_at,
            TransactionEntry::SchemaEvolution { timestamp, .. } => *timestamp,
        }
    }

    /// Get the batch ID if this entry has one.
    pub fn batch_id(&self) -> Option<&str> {
        match self {
            TransactionEntry::BufferSnapshot { batch_id, .. } => Some(batch_id),
            TransactionEntry::FlushStart { batch_id, .. } => Some(batch_id),
            TransactionEntry::ParquetWritten { batch_id, .. } => Some(batch_id),
            TransactionEntry::IcebergSnapshot { batch_id, .. } => Some(batch_id),
            TransactionEntry::FlushComplete { batch_id, .. } => Some(batch_id),
            _ => None,
        }
    }

    /// Check if this is a checkpoint entry.
    pub fn is_checkpoint(&self) -> bool {
        matches!(self, TransactionEntry::Checkpoint { .. })
    }

    /// Check if this is a flush complete entry.
    pub fn is_flush_complete(&self) -> bool {
        matches!(self, TransactionEntry::FlushComplete { .. })
    }

    /// Check if this is an idempotency record.
    pub fn is_idempotency_record(&self) -> bool {
        matches!(self, TransactionEntry::IdempotencyRecord { .. })
    }

    /// Check if this is a catalog health check entry.
    pub fn is_catalog_health_check(&self) -> bool {
        matches!(self, TransactionEntry::CatalogHealthCheck { .. })
    }

    /// Check if this is a catalog error entry.
    pub fn is_catalog_error(&self) -> bool {
        matches!(self, TransactionEntry::CatalogError { .. })
    }

    /// Check if this is a schema evolution entry.
    pub fn is_schema_evolution(&self) -> bool {
        matches!(self, TransactionEntry::SchemaEvolution { .. })
    }

    /// Check if this idempotency record covers the given offset range.
    ///
    /// Returns true if this record's offset range contains or equals the given range.
    pub fn covers_offset_range(
        &self,
        topic: &str,
        partition: i32,
        min_offset: i64,
        max_offset: i64,
    ) -> bool {
        match self {
            TransactionEntry::IdempotencyRecord {
                kafka_offset_min,
                kafka_offset_max,
                topic: record_topic,
                partition: record_partition,
                ..
            } => {
                record_topic == topic
                    && *record_partition == partition
                    && *kafka_offset_min <= min_offset
                    && *kafka_offset_max >= max_offset
            }
            _ => false,
        }
    }

    /// Get the snapshot ID if this entry has one.
    pub fn snapshot_id(&self) -> Option<i64> {
        match self {
            TransactionEntry::IcebergSnapshot { snapshot_id, .. } => Some(*snapshot_id),
            TransactionEntry::FlushComplete {
                iceberg_snapshot_id,
                ..
            } => Some(*iceberg_snapshot_id),
            TransactionEntry::IdempotencyRecord { snapshot_id, .. } => Some(*snapshot_id),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_serialization() {
        let entry = TransactionEntry::FlushComplete {
            batch_id: "test-123".into(),
            kafka_offset: 1000,
            iceberg_snapshot_id: 42,
            duration_ms: 250,
            timestamp: Utc::now(),
        };

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("FlushComplete"));
        assert!(json.contains("test-123"));

        let parsed: TransactionEntry = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_flush_complete());
        assert_eq!(parsed.batch_id(), Some("test-123"));
    }

    #[test]
    fn test_checkpoint_entry() {
        let entry = TransactionEntry::Checkpoint {
            checkpoint_id: "cp-1".into(),
            last_kafka_offset: 5000,
            last_iceberg_snapshot: 10,
            entries_since_last: 100,
            timestamp: Utc::now(),
        };

        assert!(entry.is_checkpoint());
        assert!(!entry.is_flush_complete());
    }

    #[test]
    fn test_catalog_health_check_entry() {
        let entry = TransactionEntry::CatalogHealthCheck {
            catalog_type: "rest".into(),
            is_healthy: true,
            response_time_ms: 45,
            details: Some("Connected successfully".into()),
            timestamp: Utc::now(),
        };

        assert!(entry.is_catalog_health_check());
        assert!(!entry.is_catalog_error());

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("CatalogHealthCheck"));
        assert!(json.contains("rest"));

        let parsed: TransactionEntry = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_catalog_health_check());
    }

    #[test]
    fn test_catalog_error_entry() {
        let entry = TransactionEntry::CatalogError {
            catalog_type: "rest".into(),
            error_message: "Connection timeout".into(),
            error_code: Some("ETIMEDOUT".into()),
            retry_count: 3,
            is_retryable: true,
            timestamp: Utc::now(),
        };

        assert!(entry.is_catalog_error());
        assert!(!entry.is_catalog_health_check());

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("CatalogError"));
        assert!(json.contains("Connection timeout"));

        let parsed: TransactionEntry = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_catalog_error());
    }

    #[test]
    fn test_idempotency_record_entry() {
        let entry = TransactionEntry::IdempotencyRecord {
            kafka_offset_min: 1000,
            kafka_offset_max: 2000,
            topic: "events".into(),
            partition: 0,
            snapshot_id: 12345,
            file_paths: vec!["s3://bucket/data/file1.parquet".into()],
            committed_at: Utc::now(),
        };

        assert!(entry.is_idempotency_record());
        assert_eq!(entry.snapshot_id(), Some(12345));

        // Test covers_offset_range
        assert!(entry.covers_offset_range("events", 0, 1000, 2000));
        assert!(entry.covers_offset_range("events", 0, 1500, 1600));
        assert!(!entry.covers_offset_range("events", 0, 500, 1500)); // min too low
        assert!(!entry.covers_offset_range("events", 0, 1500, 2500)); // max too high
        assert!(!entry.covers_offset_range("events", 1, 1000, 2000)); // wrong partition
        assert!(!entry.covers_offset_range("other", 0, 1000, 2000)); // wrong topic

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("IdempotencyRecord"));
        assert!(json.contains("12345"));

        let parsed: TransactionEntry = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_idempotency_record());
    }

    #[test]
    fn test_schema_evolution_entry() {
        let entry = TransactionEntry::SchemaEvolution {
            database: "analytics".into(),
            table: "events".into(),
            fields_added: vec![SchemaField {
                name: "user_id".into(),
                field_type: "string".into(),
                required: true,
                doc: Some("Unique user identifier".into()),
            }],
            fields_removed: vec!["old_field".into()],
            old_schema_id: Some(1),
            new_schema_id: 2,
            reason: "auto".into(),
            timestamp: Utc::now(),
        };

        assert!(entry.is_schema_evolution());
        assert!(!entry.is_idempotency_record());

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("SchemaEvolution"));
        assert!(json.contains("user_id"));
        assert!(json.contains("old_field"));

        let parsed: TransactionEntry = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_schema_evolution());
    }

    #[test]
    fn test_schema_field_serialization() {
        let field = SchemaField {
            name: "event_time".into(),
            field_type: "timestamp".into(),
            required: true,
            doc: Some("Event timestamp".into()),
        };

        let json = serde_json::to_string(&field).unwrap();
        assert!(json.contains("event_time"));
        assert!(json.contains("timestamp"));

        let parsed: SchemaField = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "event_time");
        assert_eq!(parsed.field_type, "timestamp");
        assert!(parsed.required);
    }

    #[test]
    fn test_snapshot_id_extraction() {
        let iceberg_entry = TransactionEntry::IcebergSnapshot {
            batch_id: "batch-1".into(),
            snapshot_id: 100,
            manifest_list_path: "s3://bucket/manifests".into(),
            row_count_total: 1000,
            timestamp: Utc::now(),
        };
        assert_eq!(iceberg_entry.snapshot_id(), Some(100));

        let flush_entry = TransactionEntry::FlushComplete {
            batch_id: "batch-1".into(),
            kafka_offset: 500,
            iceberg_snapshot_id: 200,
            duration_ms: 100,
            timestamp: Utc::now(),
        };
        assert_eq!(flush_entry.snapshot_id(), Some(200));

        let idempotency_entry = TransactionEntry::IdempotencyRecord {
            kafka_offset_min: 1,
            kafka_offset_max: 100,
            topic: "test".into(),
            partition: 0,
            snapshot_id: 300,
            file_paths: vec![],
            committed_at: Utc::now(),
        };
        assert_eq!(idempotency_entry.snapshot_id(), Some(300));

        let offset_entry = TransactionEntry::OffsetMarker {
            topic: "test".into(),
            partition: 0,
            offset: 100,
            record_count: 10,
            timestamp: Utc::now(),
        };
        assert_eq!(offset_entry.snapshot_id(), None);
    }
}
