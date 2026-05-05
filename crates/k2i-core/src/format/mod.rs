//! Wire-format decoding into Arrow batches.
//!
//! The ingestion engine polls Kafka as raw bytes, then hands each batch to a
//! format decoder. Decoders are responsible for producing Arrow columns plus
//! K2I metadata columns that preserve replay and read-state semantics.

use crate::iceberg::TableSchema;
use crate::kafka::KafkaMessage;
use crate::{BufferError, Error, Result};
use arrow::array::{BinaryBuilder, Int32Builder, Int64Builder, StringBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::sync::Arc;

pub mod protobuf;

/// Metadata columns required by the writer and read-state registry.
pub const COL_KEY: &str = "key";
pub const COL_VALUE: &str = "value";
pub const COL_TOPIC: &str = "topic";
pub const COL_PARTITION: &str = "partition";
pub const COL_OFFSET: &str = "offset";
pub const COL_TIMESTAMP: &str = "timestamp";
pub const COL_READ_LSN: &str = "read_lsn";
pub const COL_SCHEMA_ID: &str = "_k2i_schema_id";
pub const COL_MESSAGE_TYPE: &str = "_k2i_message_type";

/// A decoded Kafka batch ready for hot-buffer append and Iceberg writing.
#[derive(Debug, Clone)]
pub struct DecodedBatch {
    /// Arrow rows, including K2I metadata columns.
    pub batch: RecordBatch,
    /// Logical format name used for logging/metrics.
    pub format: &'static str,
    /// Schema registry IDs seen in the batch, if the format uses registry IDs.
    pub schema_ids: Vec<i32>,
    /// Decoded table schema, when the format carries a structured schema.
    pub table_schema: Option<TableSchema>,
}

impl DecodedBatch {
    /// Create a decoded batch.
    pub fn new(
        batch: RecordBatch,
        format: &'static str,
        schema_ids: Vec<i32>,
        table_schema: Option<TableSchema>,
    ) -> Self {
        Self {
            batch,
            format,
            schema_ids,
            table_schema,
        }
    }
}

/// Pluggable decoder used by the ingestion engine.
#[async_trait]
pub trait MessageDecoder: Send + Sync {
    /// Stable format name.
    fn format_name(&self) -> &'static str;

    /// Decode a Kafka batch into a single Arrow record batch.
    async fn decode_batch(
        &self,
        messages: &[KafkaMessage],
        read_lsns: &[u64],
    ) -> Result<DecodedBatch>;
}

/// Decoder for the existing raw Kafka byte layout.
#[derive(Debug, Default)]
pub struct RawDecoder;

impl RawDecoder {
    /// Arrow schema used by the raw byte path.
    pub fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(COL_KEY, DataType::Binary, true),
            Field::new(COL_VALUE, DataType::Binary, true),
            Field::new(COL_TOPIC, DataType::Utf8, false),
            Field::new(COL_PARTITION, DataType::Int32, false),
            Field::new(COL_OFFSET, DataType::Int64, false),
            Field::new(COL_TIMESTAMP, DataType::Int64, false),
            Field::new(COL_READ_LSN, DataType::UInt64, false),
        ]))
    }
}

#[async_trait]
impl MessageDecoder for RawDecoder {
    fn format_name(&self) -> &'static str {
        "raw"
    }

    async fn decode_batch(
        &self,
        messages: &[KafkaMessage],
        read_lsns: &[u64],
    ) -> Result<DecodedBatch> {
        if messages.len() != read_lsns.len() {
            return Err(Error::Buffer(BufferError::SchemaMismatch {
                expected: format!("{} read LSNs", messages.len()),
                actual: format!("{} read LSNs", read_lsns.len()),
            }));
        }

        let mut key_builder = BinaryBuilder::new();
        let mut value_builder = BinaryBuilder::new();
        let mut topic_builder = StringBuilder::new();
        let mut partition_builder = Int32Builder::new();
        let mut offset_builder = Int64Builder::new();
        let mut timestamp_builder = Int64Builder::new();
        let mut read_lsn_builder = UInt64Builder::new();

        for (msg, read_lsn) in messages.iter().zip(read_lsns) {
            match &msg.key {
                Some(key) => key_builder.append_value(key),
                None => key_builder.append_null(),
            }
            match &msg.value {
                Some(value) => value_builder.append_value(value),
                None => value_builder.append_null(),
            }
            topic_builder.append_value(&msg.topic);
            partition_builder.append_value(msg.partition);
            offset_builder.append_value(msg.offset);
            timestamp_builder.append_value(msg.timestamp);
            read_lsn_builder.append_value(*read_lsn);
        }

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(key_builder.finish()),
                Arc::new(value_builder.finish()),
                Arc::new(topic_builder.finish()),
                Arc::new(partition_builder.finish()),
                Arc::new(offset_builder.finish()),
                Arc::new(timestamp_builder.finish()),
                Arc::new(read_lsn_builder.finish()),
            ],
        )
        .map_err(|e| Error::Buffer(BufferError::ArrowConversion(e.to_string())))?;

        Ok(DecodedBatch::new(
            batch,
            self.format_name(),
            Vec::new(),
            None,
        ))
    }
}

/// Return true for K2I metadata columns that must not collide with user fields.
pub fn is_reserved_column(name: &str) -> bool {
    matches!(
        name,
        COL_KEY
            | COL_VALUE
            | COL_TOPIC
            | COL_PARTITION
            | COL_OFFSET
            | COL_TIMESTAMP
            | COL_READ_LSN
            | COL_SCHEMA_ID
            | COL_MESSAGE_TYPE
    ) || name.starts_with("_k2i_")
}
