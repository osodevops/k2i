//! Hot buffer with Arrow RecordBatch and DashMap index.
//!
//! The hot buffer provides sub-second data freshness by keeping recent
//! records in memory with O(1) lookup via DashMap. It supports querying
//! data before it's flushed to Iceberg.

mod eviction;
mod hot_buffer;

pub use eviction::{EvictionChecker, EvictionDecision, EvictionPolicy, RecordMetadata};
pub use hot_buffer::{BufferedRecord, HotBuffer, HotBufferStats, QueryResult, RowId};
