//! Arrow-based hot buffer with hash index.
//!
//! The hot buffer provides sub-second data freshness by keeping
//! recent records in memory with O(1) lookup via DashMap.
//!
//! ## Query API
//!
//! The hot buffer supports querying records before they are flushed to Iceberg:
//! - `query_by_key()` - Retrieve record data by message key
//! - `query_by_offset()` - Retrieve record data by partition/offset
//! - `query_range()` - Retrieve records in an offset range
//! - `query_all()` - Retrieve all buffered records

use crate::config::BufferConfig;
use crate::kafka::KafkaMessage;
use crate::{BufferError, Error, Result};
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

/// Row identifier in the buffer.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct RowId(pub usize);

/// A buffered record that can be queried.
#[derive(Debug, Clone)]
pub struct BufferedRecord {
    /// Message key (if present)
    pub key: Option<Vec<u8>>,
    /// Message value (if present)
    pub value: Option<Vec<u8>>,
    /// Kafka topic
    pub topic: String,
    /// Kafka partition
    pub partition: i32,
    /// Kafka offset
    pub offset: i64,
    /// Message timestamp
    pub timestamp: i64,
    /// Message headers
    pub headers: Vec<(String, Vec<u8>)>,
    /// When the record was inserted into the buffer
    pub inserted_at: Instant,
}

impl BufferedRecord {
    /// Create from a KafkaMessage.
    fn from_message(msg: &KafkaMessage) -> Self {
        Self {
            key: msg.key.clone(),
            value: msg.value.clone(),
            topic: msg.topic.clone(),
            partition: msg.partition,
            offset: msg.offset,
            timestamp: msg.timestamp,
            headers: msg.headers.clone(),
            inserted_at: Instant::now(),
        }
    }

    /// Get the age of this record in the buffer.
    pub fn age(&self) -> std::time::Duration {
        self.inserted_at.elapsed()
    }
}

/// Query result for hot buffer lookups.
#[derive(Debug)]
pub struct QueryResult {
    /// The matching records
    pub records: Vec<BufferedRecord>,
    /// Number of records scanned
    pub scanned: usize,
    /// Time taken for the query
    pub duration: std::time::Duration,
}

impl QueryResult {
    /// Check if the query returned any results.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Get the number of matching records.
    pub fn len(&self) -> usize {
        self.records.len()
    }
}

/// Hot buffer manager with Arrow storage and hash index.
pub struct HotBuffer {
    /// Arrow schema for the buffer
    schema: SchemaRef,

    /// Column builders (mutable during append)
    builders: RwLock<ColumnBuilders>,

    /// Raw record storage for queries (indexed by RowId)
    records: RwLock<Vec<BufferedRecord>>,

    /// Hash index: message key -> row id
    /// Using DashMap for lock-free concurrent access
    key_index: DashMap<Vec<u8>, RowId>,

    /// Offset index: (partition, offset) -> row id
    offset_index: DashMap<(i32, i64), RowId>,

    /// Buffer statistics
    stats: BufferStats,

    /// Configuration
    config: BufferConfig,

    /// Creation time (for TTL)
    created_at: Instant,
}

struct ColumnBuilders {
    key_builder: BinaryBuilder,
    value_builder: BinaryBuilder,
    topic_builder: StringBuilder,
    partition_builder: Int32Builder,
    offset_builder: Int64Builder,
    timestamp_builder: Int64Builder,
    row_count: usize,
    memory_bytes: usize,
}

#[allow(dead_code)]
struct BufferStats {
    total_records: AtomicU64,
    total_bytes: AtomicUsize,
    evictions: AtomicU64,
    flushes: AtomicU64,
}

impl HotBuffer {
    /// Create a new hot buffer.
    pub fn new(config: BufferConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, true),
            Field::new("value", DataType::Binary, true),
            Field::new("topic", DataType::Utf8, false),
            Field::new("partition", DataType::Int32, false),
            Field::new("offset", DataType::Int64, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));

        Self {
            schema,
            builders: RwLock::new(ColumnBuilders::new()),
            records: RwLock::new(Vec::new()),
            key_index: DashMap::new(),
            offset_index: DashMap::new(),
            stats: BufferStats::new(),
            config,
            created_at: Instant::now(),
        }
    }

    /// Append a Kafka message to the buffer.
    pub fn append(&self, msg: &KafkaMessage) -> Result<RowId> {
        let mut builders = self.builders.write();

        // Check capacity before append
        if builders.memory_bytes >= self.config.max_size_mb * 1_000_000 {
            return Err(Error::Buffer(BufferError::BufferFull {
                size_bytes: builders.memory_bytes,
            }));
        }

        let row_id = RowId(builders.row_count);

        // Append to Arrow builders
        match &msg.key {
            Some(k) => builders.key_builder.append_value(k),
            None => builders.key_builder.append_null(),
        }
        match &msg.value {
            Some(v) => builders.value_builder.append_value(v),
            None => builders.value_builder.append_null(),
        }
        builders.topic_builder.append_value(&msg.topic);
        builders.partition_builder.append_value(msg.partition);
        builders.offset_builder.append_value(msg.offset);
        builders.timestamp_builder.append_value(msg.timestamp);

        builders.row_count += 1;

        // Estimate memory usage
        let msg_size = msg.size_bytes();
        builders.memory_bytes += msg_size;

        drop(builders); // Release write lock before updating indexes

        // Store record for querying
        {
            let mut records = self.records.write();
            records.push(BufferedRecord::from_message(msg));
        }

        // Update indexes (lock-free operations)
        if let Some(ref key) = msg.key {
            self.key_index.insert(key.clone(), row_id);
        }
        self.offset_index
            .insert((msg.partition, msg.offset), row_id);

        // Update stats
        self.stats.total_records.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_bytes
            .fetch_add(msg_size, Ordering::Relaxed);

        Ok(row_id)
    }

    /// Look up by message key (O(1)).
    pub fn get_by_key(&self, key: &[u8]) -> Option<RowId> {
        self.key_index.get(key).map(|r| *r.value())
    }

    /// Look up by partition and offset (O(1)).
    pub fn get_by_offset(&self, partition: i32, offset: i64) -> Option<RowId> {
        self.offset_index
            .get(&(partition, offset))
            .map(|r| *r.value())
    }

    // ========================================================================
    // Query API - For sub-second freshness queries
    // ========================================================================

    /// Query a record by row ID.
    pub fn query_by_row_id(&self, row_id: RowId) -> Option<BufferedRecord> {
        let records = self.records.read();
        records.get(row_id.0).cloned()
    }

    /// Query a record by message key.
    /// Returns the record data if found, enabling sub-second freshness queries.
    pub fn query_by_key(&self, key: &[u8]) -> QueryResult {
        let start = Instant::now();
        let records = self.records.read();
        let scanned = records.len();

        let matching: Vec<BufferedRecord> = if let Some(row_id) = self.get_by_key(key) {
            records.get(row_id.0).cloned().into_iter().collect()
        } else {
            Vec::new()
        };

        QueryResult {
            records: matching,
            scanned,
            duration: start.elapsed(),
        }
    }

    /// Query a record by partition and offset.
    /// Returns the record data if found.
    pub fn query_by_offset(&self, partition: i32, offset: i64) -> QueryResult {
        let start = Instant::now();
        let records = self.records.read();
        let scanned = records.len();

        let matching: Vec<BufferedRecord> =
            if let Some(row_id) = self.get_by_offset(partition, offset) {
                records.get(row_id.0).cloned().into_iter().collect()
            } else {
                Vec::new()
            };

        QueryResult {
            records: matching,
            scanned,
            duration: start.elapsed(),
        }
    }

    /// Query records in an offset range for a partition.
    /// Useful for scanning recent data before it's flushed to Iceberg.
    pub fn query_range(&self, partition: i32, start_offset: i64, end_offset: i64) -> QueryResult {
        let start = Instant::now();
        let records = self.records.read();
        let scanned = records.len();

        let matching: Vec<BufferedRecord> = records
            .iter()
            .filter(|r| {
                r.partition == partition && r.offset >= start_offset && r.offset <= end_offset
            })
            .cloned()
            .collect();

        debug!(
            partition = partition,
            start_offset = start_offset,
            end_offset = end_offset,
            matches = matching.len(),
            "Hot buffer range query"
        );

        QueryResult {
            records: matching,
            scanned,
            duration: start.elapsed(),
        }
    }

    /// Query records by topic.
    pub fn query_by_topic(&self, topic: &str) -> QueryResult {
        let start = Instant::now();
        let records = self.records.read();
        let scanned = records.len();

        let matching: Vec<BufferedRecord> = records
            .iter()
            .filter(|r| r.topic == topic)
            .cloned()
            .collect();

        QueryResult {
            records: matching,
            scanned,
            duration: start.elapsed(),
        }
    }

    /// Query all records currently in the buffer.
    /// Use with caution for large buffers.
    pub fn query_all(&self) -> QueryResult {
        let start = Instant::now();
        let records = self.records.read();
        let scanned = records.len();

        QueryResult {
            records: records.clone(),
            scanned,
            duration: start.elapsed(),
        }
    }

    /// Query records newer than the given age.
    pub fn query_recent(&self, max_age: std::time::Duration) -> QueryResult {
        let start = Instant::now();
        let records = self.records.read();
        let scanned = records.len();

        let matching: Vec<BufferedRecord> = records
            .iter()
            .filter(|r| r.age() <= max_age)
            .cloned()
            .collect();

        QueryResult {
            records: matching,
            scanned,
            duration: start.elapsed(),
        }
    }

    /// Check if buffer should flush based on size, count, or time.
    pub fn should_flush(&self) -> bool {
        let builders = self.builders.read();

        // Size-based flush
        if builders.memory_bytes >= self.config.max_size_mb * 1_000_000 {
            return true;
        }

        // Count-based flush
        if builders.row_count >= self.config.flush_batch_size {
            return true;
        }

        // Time-based flush
        if self.created_at.elapsed().as_secs() >= self.config.flush_interval_seconds
            && builders.row_count > 0
        {
            return true;
        }

        false
    }

    /// Check if buffer is full (for backpressure).
    pub fn is_full(&self) -> bool {
        let builders = self.builders.read();
        builders.memory_bytes >= self.config.max_size_mb * 1_000_000
    }

    /// Get current memory usage.
    pub fn memory_bytes(&self) -> usize {
        self.builders.read().memory_bytes
    }

    /// Get row count.
    pub fn row_count(&self) -> usize {
        self.builders.read().row_count
    }

    /// Take a snapshot of the buffer as RecordBatch (for flushing).
    /// This clears the buffer and returns the data.
    pub fn take_snapshot(&self) -> Result<Option<RecordBatch>> {
        let mut builders = self.builders.write();

        if builders.row_count == 0 {
            return Ok(None);
        }

        let row_count = builders.row_count;

        // Build arrays from builders
        let key_array = builders.key_builder.finish();
        let value_array = builders.value_builder.finish();
        let topic_array = builders.topic_builder.finish();
        let partition_array = builders.partition_builder.finish();
        let offset_array = builders.offset_builder.finish();
        let timestamp_array = builders.timestamp_builder.finish();

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(key_array),
                Arc::new(value_array),
                Arc::new(topic_array),
                Arc::new(partition_array),
                Arc::new(offset_array),
                Arc::new(timestamp_array),
            ],
        )
        .map_err(|e| Error::Buffer(BufferError::ArrowConversion(e.to_string())))?;

        // Reset builders
        *builders = ColumnBuilders::new();

        drop(builders);

        // Clear records storage
        self.records.write().clear();

        // Clear indexes
        self.key_index.clear();
        self.offset_index.clear();

        // Update stats
        self.stats.flushes.fetch_add(1, Ordering::Relaxed);

        info!(rows = %row_count, "Hot buffer snapshot taken");

        Ok(Some(batch))
    }

    /// Get buffer statistics.
    pub fn stats(&self) -> HotBufferStats {
        let builders = self.builders.read();
        HotBufferStats {
            row_count: builders.row_count,
            memory_bytes: builders.memory_bytes,
            total_records: self.stats.total_records.load(Ordering::Relaxed),
            total_flushes: self.stats.flushes.load(Ordering::Relaxed),
            age_seconds: self.created_at.elapsed().as_secs(),
        }
    }

    /// Get the Arrow schema.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ColumnBuilders {
    fn new() -> Self {
        Self {
            key_builder: BinaryBuilder::new(),
            value_builder: BinaryBuilder::new(),
            topic_builder: StringBuilder::new(),
            partition_builder: Int32Builder::new(),
            offset_builder: Int64Builder::new(),
            timestamp_builder: Int64Builder::new(),
            row_count: 0,
            memory_bytes: 0,
        }
    }
}

impl BufferStats {
    fn new() -> Self {
        Self {
            total_records: AtomicU64::new(0),
            total_bytes: AtomicUsize::new(0),
            evictions: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
        }
    }
}

/// Hot buffer statistics.
#[derive(Debug, Clone)]
pub struct HotBufferStats {
    /// Current row count
    pub row_count: usize,
    /// Current memory usage in bytes
    pub memory_bytes: usize,
    /// Total records processed
    pub total_records: u64,
    /// Total flushes performed
    pub total_flushes: u64,
    /// Age in seconds since creation
    pub age_seconds: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_buffer() -> HotBuffer {
        let config = BufferConfig {
            ttl_seconds: 60,
            max_size_mb: 100,
            flush_interval_seconds: 30,
            flush_batch_size: 1000,
            memory_alignment_bytes: 64,
        };
        HotBuffer::new(config)
    }

    fn create_test_message(offset: i64) -> KafkaMessage {
        KafkaMessage {
            key: Some(format!("key-{}", offset).into_bytes()),
            value: Some(format!("value-{}", offset).into_bytes()),
            topic: "test".to_string(),
            partition: 0,
            offset,
            timestamp: 1234567890,
            headers: vec![],
        }
    }

    #[test]
    fn test_buffer_append() {
        let buffer = create_test_buffer();
        let msg = create_test_message(100);

        let row_id = buffer.append(&msg).unwrap();
        assert_eq!(row_id.0, 0);
        assert_eq!(buffer.row_count(), 1);
    }

    #[test]
    fn test_buffer_key_lookup() {
        let buffer = create_test_buffer();
        let msg = create_test_message(100);

        buffer.append(&msg).unwrap();

        assert!(buffer.get_by_key(b"key-100").is_some());
        assert!(buffer.get_by_key(b"key-999").is_none());
    }

    #[test]
    fn test_buffer_offset_lookup() {
        let buffer = create_test_buffer();
        let msg = create_test_message(100);

        buffer.append(&msg).unwrap();

        assert!(buffer.get_by_offset(0, 100).is_some());
        assert!(buffer.get_by_offset(0, 999).is_none());
    }

    #[test]
    fn test_buffer_snapshot() {
        let buffer = create_test_buffer();

        for i in 0..100 {
            let msg = create_test_message(i);
            buffer.append(&msg).unwrap();
        }

        let batch = buffer.take_snapshot().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 100);

        // Buffer should be empty after snapshot
        assert_eq!(buffer.row_count(), 0);
        assert!(buffer.get_by_key(b"key-0").is_none());
    }

    #[test]
    fn test_buffer_empty_snapshot() {
        let buffer = create_test_buffer();
        let batch = buffer.take_snapshot().unwrap();
        assert!(batch.is_none());
    }

    // ========================================================================
    // Query API Tests
    // ========================================================================

    #[test]
    fn test_query_by_key() {
        let buffer = create_test_buffer();
        let msg = create_test_message(100);
        buffer.append(&msg).unwrap();

        // Query existing key
        let result = buffer.query_by_key(b"key-100");
        assert_eq!(result.len(), 1);
        assert_eq!(result.records[0].offset, 100);
        assert_eq!(result.records[0].value, Some(b"value-100".to_vec()));

        // Query non-existent key
        let result = buffer.query_by_key(b"key-999");
        assert!(result.is_empty());
    }

    #[test]
    fn test_query_by_offset() {
        let buffer = create_test_buffer();
        let msg = create_test_message(100);
        buffer.append(&msg).unwrap();

        // Query existing offset
        let result = buffer.query_by_offset(0, 100);
        assert_eq!(result.len(), 1);
        assert_eq!(result.records[0].key, Some(b"key-100".to_vec()));

        // Query non-existent offset
        let result = buffer.query_by_offset(0, 999);
        assert!(result.is_empty());
    }

    #[test]
    fn test_query_by_row_id() {
        let buffer = create_test_buffer();
        let msg = create_test_message(100);
        let row_id = buffer.append(&msg).unwrap();

        // Query by row ID
        let record = buffer.query_by_row_id(row_id);
        assert!(record.is_some());
        let record = record.unwrap();
        assert_eq!(record.offset, 100);

        // Query invalid row ID
        let record = buffer.query_by_row_id(RowId(999));
        assert!(record.is_none());
    }

    #[test]
    fn test_query_range() {
        let buffer = create_test_buffer();

        // Add multiple messages
        for i in 0..10 {
            let msg = create_test_message(i);
            buffer.append(&msg).unwrap();
        }

        // Query a range
        let result = buffer.query_range(0, 3, 7);
        assert_eq!(result.len(), 5); // offsets 3, 4, 5, 6, 7

        // Verify offset ordering
        let offsets: Vec<i64> = result.records.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, vec![3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_query_by_topic() {
        let buffer = create_test_buffer();

        // Add messages with default topic "test"
        for i in 0..5 {
            let msg = create_test_message(i);
            buffer.append(&msg).unwrap();
        }

        // Query by topic
        let result = buffer.query_by_topic("test");
        assert_eq!(result.len(), 5);

        // Query non-existent topic
        let result = buffer.query_by_topic("other");
        assert!(result.is_empty());
    }

    #[test]
    fn test_query_all() {
        let buffer = create_test_buffer();

        // Add multiple messages
        for i in 0..10 {
            let msg = create_test_message(i);
            buffer.append(&msg).unwrap();
        }

        let result = buffer.query_all();
        assert_eq!(result.len(), 10);
        assert_eq!(result.scanned, 10);
    }

    #[test]
    fn test_query_recent() {
        let buffer = create_test_buffer();

        // Add messages
        for i in 0..5 {
            let msg = create_test_message(i);
            buffer.append(&msg).unwrap();
        }

        // All records should be very recent (< 1 second)
        let result = buffer.query_recent(std::time::Duration::from_secs(1));
        assert_eq!(result.len(), 5);

        // No records should be older than 0 seconds
        let result = buffer.query_recent(std::time::Duration::from_nanos(0));
        assert!(result.is_empty());
    }

    #[test]
    fn test_buffered_record_age() {
        let buffer = create_test_buffer();
        let msg = create_test_message(100);
        buffer.append(&msg).unwrap();

        let result = buffer.query_by_key(b"key-100");
        assert!(!result.is_empty());

        // Age should be very small (just inserted)
        let age = result.records[0].age();
        assert!(age < std::time::Duration::from_secs(1));
    }

    #[test]
    fn test_query_after_snapshot_clears_records() {
        let buffer = create_test_buffer();

        for i in 0..10 {
            let msg = create_test_message(i);
            buffer.append(&msg).unwrap();
        }

        // Verify records exist before snapshot
        let result = buffer.query_all();
        assert_eq!(result.len(), 10);

        // Take snapshot (clears buffer)
        let _ = buffer.take_snapshot().unwrap();

        // Verify records are cleared
        let result = buffer.query_all();
        assert!(result.is_empty());

        // Verify key lookups also fail
        let result = buffer.query_by_key(b"key-0");
        assert!(result.is_empty());
    }

    #[test]
    fn test_query_result_metadata() {
        let buffer = create_test_buffer();

        for i in 0..10 {
            let msg = create_test_message(i);
            buffer.append(&msg).unwrap();
        }

        let result = buffer.query_range(0, 0, 4);
        assert_eq!(result.scanned, 10); // Scanned all records
        assert_eq!(result.len(), 5); // Found 5 matches
        assert!(result.duration.as_nanos() > 0); // Duration was tracked
    }
}
