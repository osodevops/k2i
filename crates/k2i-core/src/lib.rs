//! K2I Core - Kafka to Iceberg streaming ingestion engine
//!
//! This library provides the core functionality for ingesting events from
//! Apache Kafka and writing them to Apache Iceberg tables with:
//!
//! - Sub-second data freshness via hot buffer architecture
//! - Fully automated table maintenance
//! - Guaranteed consistency via transaction log
//! - Dual partitioning (preserve Kafka + optimize Iceberg)

pub mod buffer;
pub mod circuit_breaker;
pub mod config;
pub mod engine;
pub mod error;
pub mod health;
pub mod iceberg;
pub mod kafka;
pub mod maintenance;
pub mod metrics;
pub mod txlog;

// Re-export commonly used types
pub use config::Config;
pub use error::{BufferError, IcebergError, KafkaError, TransactionLogError};
pub use error::{Error, Result};
