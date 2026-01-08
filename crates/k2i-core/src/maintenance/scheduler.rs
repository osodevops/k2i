//! Background maintenance task scheduler.
//!
//! Manages periodic maintenance tasks:
//! - Compaction: Merge small Parquet files
//! - Snapshot expiration: Remove old snapshots
//! - Orphan cleanup: Delete unreferenced files
//! - Statistics update: Refresh column statistics

use super::compaction::CompactionTask;
use super::expiration::ExpirationTask;
use super::orphan::OrphanCleanupTask;
use super::statistics::StatisticsTask;
use crate::config::MaintenanceConfig;
use crate::txlog::TransactionLog;
use crate::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

/// Status of a maintenance task.
#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    /// Task is idle, waiting for next run
    Idle,
    /// Task is currently running
    Running,
    /// Task completed successfully
    Completed { duration: Duration },
    /// Task failed
    Failed { error: String },
    /// Task is disabled
    Disabled,
}

/// Information about a scheduled task.
#[derive(Debug, Clone)]
pub struct TaskInfo {
    /// Task name
    pub name: String,
    /// Current status
    pub status: TaskStatus,
    /// Last run time
    pub last_run: Option<Instant>,
    /// Next scheduled run
    pub next_run: Option<Instant>,
    /// Total successful runs
    pub success_count: u64,
    /// Total failed runs
    pub failure_count: u64,
}

/// Maintenance task scheduler.
pub struct MaintenanceScheduler {
    config: MaintenanceConfig,
    txlog: Option<Arc<TransactionLog>>,
    shutdown_tx: broadcast::Sender<()>,
    tasks: RwLock<HashMap<String, TaskInfo>>,
    handles: RwLock<Vec<JoinHandle<()>>>,
}

impl MaintenanceScheduler {
    /// Create a new maintenance scheduler.
    pub fn new(config: MaintenanceConfig) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            config,
            txlog: None,
            shutdown_tx,
            tasks: RwLock::new(HashMap::new()),
            handles: RwLock::new(Vec::new()),
        }
    }

    /// Create a scheduler with transaction log integration.
    pub fn with_txlog(config: MaintenanceConfig, txlog: Arc<TransactionLog>) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            config,
            txlog: Some(txlog),
            shutdown_tx,
            tasks: RwLock::new(HashMap::new()),
            handles: RwLock::new(Vec::new()),
        }
    }

    /// Start the maintenance scheduler.
    pub async fn start(self: Arc<Self>) {
        info!("Starting maintenance scheduler");

        // Register and start compaction task
        if self.config.compaction_enabled {
            self.register_task("compaction", TaskStatus::Idle);
            let handle = self.spawn_compaction_task();
            self.handles.write().push(handle);
        } else {
            self.register_task("compaction", TaskStatus::Disabled);
        }

        // Register and start expiration task
        if self.config.snapshot_expiration_enabled {
            self.register_task("expiration", TaskStatus::Idle);
            let handle = self.spawn_expiration_task();
            self.handles.write().push(handle);
        } else {
            self.register_task("expiration", TaskStatus::Disabled);
        }

        // Register and start orphan cleanup task
        if self.config.orphan_cleanup_enabled {
            self.register_task("orphan_cleanup", TaskStatus::Idle);
            let handle = self.spawn_orphan_cleanup_task();
            self.handles.write().push(handle);
        } else {
            self.register_task("orphan_cleanup", TaskStatus::Disabled);
        }

        // Register and start statistics task
        if self.config.statistics_enabled {
            self.register_task("statistics", TaskStatus::Idle);
            let handle = self.spawn_statistics_task();
            self.handles.write().push(handle);
        } else {
            self.register_task("statistics", TaskStatus::Disabled);
        }

        info!(
            compaction = self.config.compaction_enabled,
            expiration = self.config.snapshot_expiration_enabled,
            orphan_cleanup = self.config.orphan_cleanup_enabled,
            statistics = self.config.statistics_enabled,
            "Maintenance scheduler started"
        );
    }

    /// Stop the maintenance scheduler gracefully.
    pub async fn stop(&self) {
        info!("Stopping maintenance scheduler");

        // Send shutdown signal to all tasks
        let _ = self.shutdown_tx.send(());

        // Wait for all tasks to complete (with timeout)
        let handles: Vec<_> = std::mem::take(&mut *self.handles.write());
        for handle in handles {
            let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
        }

        info!("Maintenance scheduler stopped");
    }

    /// Register a task for tracking.
    fn register_task(&self, name: &str, status: TaskStatus) {
        let info = TaskInfo {
            name: name.to_string(),
            status,
            last_run: None,
            next_run: None,
            success_count: 0,
            failure_count: 0,
        };
        self.tasks.write().insert(name.to_string(), info);
    }

    /// Update task status.
    fn update_task_status(&self, name: &str, status: TaskStatus) {
        if let Some(task) = self.tasks.write().get_mut(name) {
            match &status {
                TaskStatus::Completed { .. } => {
                    task.success_count += 1;
                    task.last_run = Some(Instant::now());
                }
                TaskStatus::Failed { .. } => {
                    task.failure_count += 1;
                    task.last_run = Some(Instant::now());
                }
                _ => {}
            }
            task.status = status;
        }
    }

    /// Get status of all tasks.
    pub fn get_task_statuses(&self) -> HashMap<String, TaskInfo> {
        self.tasks.read().clone()
    }

    /// Get status of a specific task.
    pub fn get_task_status(&self, name: &str) -> Option<TaskInfo> {
        self.tasks.read().get(name).cloned()
    }

    /// Spawn the compaction task.
    fn spawn_compaction_task(self: &Arc<Self>) -> JoinHandle<()> {
        let scheduler = Arc::clone(self);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let interval = Duration::from_secs(self.config.compaction_interval_seconds);

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            // Skip the first immediate tick
            interval_timer.tick().await;

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        scheduler.run_compaction().await;
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Compaction task received shutdown signal");
                        break;
                    }
                }
            }
        })
    }

    /// Spawn the expiration task.
    fn spawn_expiration_task(self: &Arc<Self>) -> JoinHandle<()> {
        let scheduler = Arc::clone(self);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        // Run expiration daily (86400 seconds)
        let interval = Duration::from_secs(86400);

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.tick().await;

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        scheduler.run_expiration().await;
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Expiration task received shutdown signal");
                        break;
                    }
                }
            }
        })
    }

    /// Spawn the orphan cleanup task.
    fn spawn_orphan_cleanup_task(self: &Arc<Self>) -> JoinHandle<()> {
        let scheduler = Arc::clone(self);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        // Run orphan cleanup daily (86400 seconds)
        let interval = Duration::from_secs(86400);

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.tick().await;

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        scheduler.run_orphan_cleanup().await;
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Orphan cleanup task received shutdown signal");
                        break;
                    }
                }
            }
        })
    }

    /// Spawn the statistics update task.
    fn spawn_statistics_task(self: &Arc<Self>) -> JoinHandle<()> {
        let scheduler = Arc::clone(self);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let interval = Duration::from_secs(self.config.statistics_interval_seconds);

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.tick().await;

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        scheduler.run_statistics().await;
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Statistics task received shutdown signal");
                        break;
                    }
                }
            }
        })
    }

    /// Run compaction task.
    async fn run_compaction(&self) {
        self.update_task_status("compaction", TaskStatus::Running);
        let start = Instant::now();

        debug!("Starting compaction task");

        let task = CompactionTask::new(
            self.config.compaction_threshold_mb,
            self.config.compaction_target_mb,
            self.txlog.clone(),
        );

        match task.run().await {
            Ok(result) => {
                let duration = start.elapsed();
                info!(
                    files_compacted = result.files_compacted,
                    bytes_saved = result.bytes_saved,
                    duration_ms = duration.as_millis(),
                    "Compaction completed"
                );
                self.update_task_status("compaction", TaskStatus::Completed { duration });
            }
            Err(e) => {
                error!(error = %e, "Compaction failed");
                self.update_task_status(
                    "compaction",
                    TaskStatus::Failed {
                        error: e.to_string(),
                    },
                );
            }
        }
    }

    /// Run expiration task.
    async fn run_expiration(&self) {
        self.update_task_status("expiration", TaskStatus::Running);
        let start = Instant::now();

        debug!("Starting expiration task");

        let task = ExpirationTask::new(self.config.snapshot_retention_days, self.txlog.clone());

        match task.run().await {
            Ok(result) => {
                let duration = start.elapsed();
                info!(
                    snapshots_expired = result.snapshots_expired,
                    manifests_deleted = result.manifests_deleted,
                    duration_ms = duration.as_millis(),
                    "Expiration completed"
                );
                self.update_task_status("expiration", TaskStatus::Completed { duration });
            }
            Err(e) => {
                error!(error = %e, "Expiration failed");
                self.update_task_status(
                    "expiration",
                    TaskStatus::Failed {
                        error: e.to_string(),
                    },
                );
            }
        }
    }

    /// Run orphan cleanup task.
    async fn run_orphan_cleanup(&self) {
        self.update_task_status("orphan_cleanup", TaskStatus::Running);
        let start = Instant::now();

        debug!("Starting orphan cleanup task");

        let task = OrphanCleanupTask::new(self.config.orphan_retention_days, self.txlog.clone());

        match task.run().await {
            Ok(result) => {
                let duration = start.elapsed();
                info!(
                    files_deleted = result.files_deleted,
                    bytes_freed = result.bytes_freed,
                    duration_ms = duration.as_millis(),
                    "Orphan cleanup completed"
                );
                self.update_task_status("orphan_cleanup", TaskStatus::Completed { duration });
            }
            Err(e) => {
                error!(error = %e, "Orphan cleanup failed");
                self.update_task_status(
                    "orphan_cleanup",
                    TaskStatus::Failed {
                        error: e.to_string(),
                    },
                );
            }
        }
    }

    /// Run statistics update task.
    async fn run_statistics(&self) {
        self.update_task_status("statistics", TaskStatus::Running);
        let start = Instant::now();

        debug!("Starting statistics update task");

        let task = StatisticsTask::new(self.txlog.clone());

        match task.run().await {
            Ok(result) => {
                let duration = start.elapsed();
                info!(
                    files_analyzed = result.files_analyzed,
                    columns_updated = result.columns_updated,
                    total_rows = result.total_rows,
                    duration_ms = duration.as_millis(),
                    "Statistics update completed"
                );
                self.update_task_status("statistics", TaskStatus::Completed { duration });
            }
            Err(e) => {
                error!(error = %e, "Statistics update failed");
                self.update_task_status(
                    "statistics",
                    TaskStatus::Failed {
                        error: e.to_string(),
                    },
                );
            }
        }
    }

    /// Trigger compaction manually (for testing or on-demand).
    pub async fn trigger_compaction(&self) -> Result<CompactionResult> {
        let task = CompactionTask::new(
            self.config.compaction_threshold_mb,
            self.config.compaction_target_mb,
            self.txlog.clone(),
        );
        task.run().await
    }

    /// Trigger expiration manually.
    pub async fn trigger_expiration(&self) -> Result<ExpirationResult> {
        let task = ExpirationTask::new(self.config.snapshot_retention_days, self.txlog.clone());
        task.run().await
    }

    /// Trigger orphan cleanup manually.
    pub async fn trigger_orphan_cleanup(&self) -> Result<OrphanCleanupResult> {
        let task = OrphanCleanupTask::new(self.config.orphan_retention_days, self.txlog.clone());
        task.run().await
    }

    /// Trigger statistics update manually.
    pub async fn trigger_statistics(&self) -> Result<StatisticsResult> {
        let task = StatisticsTask::new(self.txlog.clone());
        task.run().await
    }
}

/// Result of a compaction operation.
#[derive(Debug, Clone, Default)]
pub struct CompactionResult {
    /// Number of files compacted
    pub files_compacted: usize,
    /// Number of new files created
    pub files_created: usize,
    /// Bytes saved by compaction
    pub bytes_saved: u64,
    /// Files that were processed
    pub processed_files: Vec<String>,
}

/// Result of an expiration operation.
#[derive(Debug, Clone, Default)]
pub struct ExpirationResult {
    /// Number of snapshots expired
    pub snapshots_expired: usize,
    /// Number of manifests deleted
    pub manifests_deleted: usize,
    /// Snapshot IDs that were expired
    pub expired_snapshot_ids: Vec<i64>,
}

/// Result of an orphan cleanup operation.
#[derive(Debug, Clone, Default)]
pub struct OrphanCleanupResult {
    /// Number of orphan files deleted
    pub files_deleted: usize,
    /// Total bytes freed
    pub bytes_freed: u64,
    /// Paths of deleted files
    pub deleted_paths: Vec<String>,
}

/// Result of a statistics update operation.
#[derive(Debug, Clone, Default)]
pub struct StatisticsResult {
    /// Number of files analyzed
    pub files_analyzed: usize,
    /// Number of columns with updated statistics
    pub columns_updated: usize,
    /// Total rows across all analyzed files
    pub total_rows: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MaintenanceConfig;

    fn create_test_config() -> MaintenanceConfig {
        MaintenanceConfig {
            compaction_enabled: true,
            compaction_interval_seconds: 3600,
            compaction_threshold_mb: 100,
            compaction_target_mb: 512,
            snapshot_expiration_enabled: true,
            snapshot_retention_days: 7,
            orphan_cleanup_enabled: true,
            orphan_retention_days: 3,
            statistics_enabled: true,
            statistics_interval_seconds: 3600,
        }
    }

    #[test]
    fn test_scheduler_creation() {
        let config = create_test_config();
        let scheduler = MaintenanceScheduler::new(config);

        let statuses = scheduler.get_task_statuses();
        assert!(statuses.is_empty()); // No tasks registered until start
    }

    #[tokio::test]
    async fn test_scheduler_task_registration() {
        let config = create_test_config();
        let scheduler = Arc::new(MaintenanceScheduler::new(config));

        scheduler.clone().start().await;

        let statuses = scheduler.get_task_statuses();
        assert!(statuses.contains_key("compaction"));
        assert!(statuses.contains_key("expiration"));
        assert!(statuses.contains_key("orphan_cleanup"));
        assert!(statuses.contains_key("statistics"));

        scheduler.stop().await;
    }

    #[tokio::test]
    async fn test_scheduler_disabled_tasks() {
        let mut config = create_test_config();
        config.compaction_enabled = false;
        config.snapshot_expiration_enabled = false;
        config.orphan_cleanup_enabled = false;

        let scheduler = Arc::new(MaintenanceScheduler::new(config));
        scheduler.clone().start().await;

        let statuses = scheduler.get_task_statuses();
        assert_eq!(
            statuses.get("compaction").unwrap().status,
            TaskStatus::Disabled
        );
        assert_eq!(
            statuses.get("expiration").unwrap().status,
            TaskStatus::Disabled
        );
        assert_eq!(
            statuses.get("orphan_cleanup").unwrap().status,
            TaskStatus::Disabled
        );

        scheduler.stop().await;
    }

    #[test]
    fn test_task_info_default() {
        let info = TaskInfo {
            name: "test".to_string(),
            status: TaskStatus::Idle,
            last_run: None,
            next_run: None,
            success_count: 0,
            failure_count: 0,
        };

        assert_eq!(info.name, "test");
        assert_eq!(info.status, TaskStatus::Idle);
        assert_eq!(info.success_count, 0);
    }
}
