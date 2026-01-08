//! Prometheus metrics export.

mod prometheus;

pub use self::prometheus::{ErrorType, FlushDurationHistogram, IngestionMetrics};
