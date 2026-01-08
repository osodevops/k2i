//! Automated maintenance tasks for Iceberg tables.
//!
//! This module provides background maintenance capabilities:
//! - **Compaction**: Merge small Parquet files into larger ones
//! - **Expiration**: Remove old snapshots beyond retention period
//! - **Orphan Cleanup**: Delete unreferenced files
//! - **Statistics**: Update column statistics for query optimization

mod compaction;
mod expiration;
mod orphan;
mod scheduler;
mod statistics;

pub use compaction::{CompactionTask, FileInfo};
pub use expiration::{ExpirationTask, SnapshotInfo, SnapshotSummary};
pub use orphan::{OrphanCandidate, OrphanCleanupTask, OrphanFileType, OrphanSummary};
pub use scheduler::{
    CompactionResult, ExpirationResult, MaintenanceScheduler, OrphanCleanupResult,
    StatisticsResult, TaskInfo, TaskStatus,
};
pub use statistics::{ColumnStatistics, StatisticsTask};
