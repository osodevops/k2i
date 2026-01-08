//! Transaction log for crash recovery and consistency.
//!
//! The transaction log provides:
//! - Append-only logging for crash recovery
//! - Automatic checkpointing with state tracking
//! - Log rotation and cleanup
//! - Recovery with orphan file detection

mod entries;
mod log;
mod recovery;

pub use entries::{MaintenanceOp, TransactionEntry};
pub use log::TransactionLog;
pub use recovery::{CleanupResult, OrphanFile, OrphanReason, RecoveryState, RecoverySummary};
