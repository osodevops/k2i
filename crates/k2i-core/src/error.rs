//! Error types for k2i core library.
//!
//! Uses hierarchical domain-specific errors following the thiserror pattern.

use thiserror::Error;

/// Result type alias for k2i operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Top-level error type for k2i.
#[derive(Error, Debug)]
pub enum Error {
    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Kafka-related error
    #[error("Kafka error: {0}")]
    Kafka(#[from] KafkaError),

    /// Buffer-related error
    #[error("Buffer error: {0}")]
    Buffer(#[from] BufferError),

    /// Iceberg-related error
    #[error("Iceberg error: {0}")]
    Iceberg(#[from] IcebergError),

    /// Transaction log error
    #[error("Transaction log error: {0}")]
    TransactionLog(#[from] TransactionLogError),

    /// Storage error
    #[error("Storage error: {0}")]
    Storage(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Shutdown requested
    #[error("Shutdown requested")]
    Shutdown,
}

/// Kafka-specific errors.
#[derive(Error, Debug)]
pub enum KafkaError {
    /// Failed to connect to broker
    #[error("Connection failed to {broker}: {message}")]
    ConnectionFailed { broker: String, message: String },

    /// Consumer group error
    #[error("Consumer group error: {0}")]
    ConsumerGroup(String),

    /// Offset commit failed
    #[error("Offset commit failed: {0}")]
    OffsetCommit(String),

    /// Partition assignment error
    #[error("Partition assignment error: {0}")]
    PartitionAssignment(String),

    /// Timeout error
    #[error("Timeout: {0}")]
    Timeout(String),

    /// Backpressure engaged
    #[error("Backpressure engaged: buffer full")]
    BackpressureEngaged,

    /// Message parse error
    #[error("Message parse error: {0}")]
    MessageParse(String),

    /// Consumer closed
    #[error("Consumer closed")]
    ConsumerClosed,
}

/// Buffer-specific errors.
#[derive(Error, Debug)]
pub enum BufferError {
    /// Buffer is full
    #[error("Buffer full: {size_bytes} bytes")]
    BufferFull { size_bytes: usize },

    /// Arrow conversion error
    #[error("Arrow conversion error: {0}")]
    ArrowConversion(String),

    /// Schema mismatch
    #[error("Schema mismatch: expected {expected}, got {actual}")]
    SchemaMismatch { expected: String, actual: String },

    /// Memory alignment error
    #[error("Memory alignment error: {0}")]
    MemoryAlignment(String),

    /// Buffer is empty
    #[error("Buffer is empty")]
    Empty,
}

/// Iceberg-specific errors.
#[derive(Error, Debug)]
pub enum IcebergError {
    /// Catalog connection failed
    #[error("Catalog connection failed: {0}")]
    CatalogConnection(String),

    /// Table not found
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// Snapshot commit failed
    #[error("Snapshot commit failed: {0}")]
    SnapshotCommit(String),

    /// Parquet write error
    #[error("Parquet write error: {0}")]
    ParquetWrite(String),

    /// Schema evolution error
    #[error("Schema evolution error: {0}")]
    SchemaEvolution(String),

    /// Compare-and-set conflict
    #[error("CAS conflict: expected snapshot {expected}, found {actual}")]
    CasConflict { expected: i64, actual: i64 },

    /// File upload error
    #[error("File upload error: {0}")]
    FileUpload(String),

    /// Schema mismatch between expected and actual
    #[error("Schema mismatch: expected {expected}, actual {actual}")]
    SchemaMismatch { expected: String, actual: String },

    /// Generic Iceberg error
    #[error("Iceberg error: {0}")]
    Other(String),
}

/// Transaction log errors.
#[derive(Error, Debug)]
pub enum TransactionLogError {
    /// Log corrupted
    #[error("Log corrupted at position {position}: {message}")]
    Corrupted { position: u64, message: String },

    /// Recovery failed
    #[error("Recovery failed: {0}")]
    RecoveryFailed(String),

    /// Checkpoint failed
    #[error("Checkpoint failed: {0}")]
    CheckpointFailed(String),

    /// Entry write failed
    #[error("Entry write failed: {0}")]
    WriteFailed(String),

    /// Checksum mismatch
    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },
}

// Conversion implementations for external error types

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<toml::de::Error> for Error {
    fn from(err: toml::de::Error) -> Self {
        Error::Config(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::Config("invalid value".into());
        assert_eq!(err.to_string(), "Configuration error: invalid value");

        let kafka_err = KafkaError::ConnectionFailed {
            broker: "localhost:9092".into(),
            message: "connection refused".into(),
        };
        let err: Error = kafka_err.into();
        assert!(err.to_string().contains("Connection failed"));
    }

    #[test]
    fn test_buffer_error() {
        let err = BufferError::BufferFull { size_bytes: 1024 };
        assert_eq!(err.to_string(), "Buffer full: 1024 bytes");
    }

    #[test]
    fn test_iceberg_error() {
        let err = IcebergError::CasConflict {
            expected: 100,
            actual: 101,
        };
        assert!(err.to_string().contains("CAS conflict"));
    }
}
