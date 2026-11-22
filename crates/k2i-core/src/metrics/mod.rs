//! Prometheus metrics export.

pub mod otel;
mod prometheus;

pub use self::prometheus::{ErrorType, FlushDurationHistogram, IngestionMetrics};
