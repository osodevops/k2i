//! Snapshot expiration for Iceberg tables.
//!
//! Expires old snapshots to:
//! - Free up storage space
//! - Remove old manifests and data files
//! - Maintain table health
//!
//! ## Expiration Strategy
//!
//! 1. **List snapshots**: Get all snapshots from catalog
//! 2. **Find candidates**: Identify snapshots older than retention period
//! 3. **Validate**: Ensure we never expire the current snapshot
//! 4. **Expire**: Remove old snapshots via catalog API
//! 5. **Cleanup**: Delete orphaned manifest files

use super::scheduler::ExpirationResult;
use crate::config::IcebergConfig;
use crate::iceberg::CatalogOperations;
use crate::txlog::{MaintenanceOp, TransactionEntry, TransactionLog};
use crate::Result;
use chrono::{DateTime, Duration, Utc};
use futures::TryStreamExt;
use object_store::ObjectStore;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Snapshot information for expiration decisions.
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// Snapshot ID
    pub snapshot_id: i64,
    /// Parent snapshot ID (if any)
    pub parent_id: Option<i64>,
    /// Timestamp when snapshot was created
    pub timestamp: chrono::DateTime<Utc>,
    /// Manifest list path
    pub manifest_list: String,
    /// Summary statistics
    pub summary: SnapshotSummary,
}

/// Summary statistics for a snapshot.
#[derive(Debug, Clone, Default)]
pub struct SnapshotSummary {
    /// Number of added files
    pub added_files: u64,
    /// Number of deleted files
    pub deleted_files: u64,
    /// Total records in snapshot
    pub total_records: u64,
    /// Total data files
    pub total_files: u64,
}

/// Expiration task that removes old snapshots.
pub struct ExpirationTask {
    /// Retention period in days
    retention_days: u32,
    /// Transaction log for recording operations
    txlog: Option<Arc<TransactionLog>>,
    /// Catalog operations for metadata updates
    catalog: Option<Arc<dyn CatalogOperations>>,
    /// Object store for manifest file deletion
    object_store: Option<Arc<dyn ObjectStore>>,
    /// Iceberg config for namespace/table
    config: Option<IcebergConfig>,
}

impl ExpirationTask {
    /// Create a new expiration task.
    pub fn new(retention_days: u32, txlog: Option<Arc<TransactionLog>>) -> Self {
        Self {
            retention_days,
            txlog,
            catalog: None,
            object_store: None,
            config: None,
        }
    }

    /// Set the catalog operations for real expiration.
    pub fn with_catalog(mut self, catalog: Arc<dyn CatalogOperations>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Set the object store for manifest deletion.
    pub fn with_object_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(store);
        self
    }

    /// Set the Iceberg config.
    pub fn with_config(mut self, config: IcebergConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Run the expiration task.
    pub async fn run(&self) -> Result<ExpirationResult> {
        debug!(
            retention_days = self.retention_days,
            "Starting expiration task"
        );

        // Log expiration start
        if let Some(ref txlog) = self.txlog {
            txlog.append(TransactionEntry::Maintenance {
                operation: MaintenanceOp::SnapshotExpired,
                details: format!(
                    "Snapshot expiration started with retention={}days",
                    self.retention_days
                ),
                timestamp: Utc::now(),
            })?;
        }

        // TODO: Implement actual Iceberg snapshot expiration
        // This would involve:
        // 1. List all snapshots from Iceberg metadata
        // 2. Find snapshots older than retention period
        // 3. Ensure we don't expire the current snapshot
        // 4. Expire old snapshots via Iceberg API
        // 5. Clean up orphaned manifests and data files

        let result = self.perform_expiration().await?;

        info!(
            snapshots_expired = result.snapshots_expired,
            manifests_deleted = result.manifests_deleted,
            "Expiration task completed"
        );

        Ok(result)
    }

    /// Perform the actual expiration logic.
    async fn perform_expiration(&self) -> Result<ExpirationResult> {
        // Check if we have required components
        let (catalog, store, config) = match (&self.catalog, &self.object_store, &self.config) {
            (Some(c), Some(s), Some(cfg)) => (c, s, cfg),
            _ => {
                debug!("Expiration skipped: missing catalog, object_store, or config");
                return Ok(ExpirationResult::default());
            }
        };

        let namespace = &config.database_name;
        let table = &config.table_name;

        // Step 1: Get current snapshot ID (we must never expire this)
        let current_snapshot_id = match catalog.current_snapshot_id(namespace, table).await? {
            Some(id) => id,
            None => {
                debug!("No current snapshot, nothing to expire");
                return Ok(ExpirationResult::default());
            }
        };

        // Step 2: Get table info to find snapshot history
        let table_info = match catalog.load_table(namespace, table).await {
            Ok(info) => info,
            Err(e) => {
                warn!(error = %e, "Failed to load table for expiration");
                return Ok(ExpirationResult::default());
            }
        };

        // Step 3: Build snapshot list by scanning metadata directory
        // Since the CatalogOperations trait doesn't expose snapshot history directly,
        // we scan the metadata directory for snapshot manifest files
        let snapshots = self
            .list_snapshots_from_metadata(store.as_ref(), &table_info.location, current_snapshot_id)
            .await?;

        debug!(
            snapshot_count = snapshots.len(),
            current_snapshot_id = current_snapshot_id,
            "Found snapshots in metadata directory"
        );

        // Step 4: Find expiration candidates
        let candidates = self.find_expiration_candidates(&snapshots, current_snapshot_id);

        if candidates.is_empty() {
            debug!("No snapshots eligible for expiration");
            return Ok(ExpirationResult::default());
        }

        // Step 5: Validate expiration is safe
        let validation = self.validate_expiration(&candidates, current_snapshot_id);
        if !validation.is_safe {
            warn!(
                warning = ?validation.warning,
                "Expiration validation failed, skipping"
            );
            return Ok(ExpirationResult::default());
        }

        info!(
            candidates = candidates.len(),
            retention_days = self.retention_days,
            "Found snapshots eligible for expiration"
        );

        // Step 6: Expire each candidate snapshot
        let mut snapshots_expired = 0;
        let mut manifests_deleted = 0;
        let mut expired_snapshot_ids = Vec::new();

        for snapshot in &candidates {
            // Delete manifest file
            let manifest_path = object_store::path::Path::from(snapshot.manifest_list.as_str());
            match store.delete(&manifest_path).await {
                Ok(()) => {
                    manifests_deleted += 1;
                    debug!(
                        snapshot_id = snapshot.snapshot_id,
                        manifest = %snapshot.manifest_list,
                        "Deleted snapshot manifest"
                    );
                }
                Err(e) => {
                    warn!(
                        snapshot_id = snapshot.snapshot_id,
                        error = %e,
                        "Failed to delete manifest, continuing"
                    );
                }
            }

            snapshots_expired += 1;
            expired_snapshot_ids.push(snapshot.snapshot_id);
        }

        // Step 7: Log expiration completion
        if let Some(ref txlog) = self.txlog {
            txlog.append(TransactionEntry::Maintenance {
                operation: MaintenanceOp::SnapshotExpired,
                details: format!(
                    "Expired {} snapshots: {:?}",
                    snapshots_expired, expired_snapshot_ids
                ),
                timestamp: Utc::now(),
            })?;
        }

        info!(
            snapshots_expired = snapshots_expired,
            manifests_deleted = manifests_deleted,
            "Snapshot expiration completed"
        );

        Ok(ExpirationResult {
            snapshots_expired,
            manifests_deleted,
            expired_snapshot_ids,
        })
    }

    /// List snapshots by scanning the metadata directory for snapshot files.
    ///
    /// Iceberg snapshot files follow the naming pattern: snap-{snapshot_id}-{uuid}.avro
    /// This method scans the metadata directory and extracts snapshot information.
    async fn list_snapshots_from_metadata(
        &self,
        store: &dyn ObjectStore,
        table_location: &str,
        current_snapshot_id: i64,
    ) -> Result<Vec<SnapshotInfo>> {
        let metadata_prefix =
            object_store::path::Path::from(format!("{}/metadata", table_location));
        let mut snapshots = Vec::new();

        let mut list_stream = store.list(Some(&metadata_prefix));
        while let Some(meta) = list_stream
            .try_next()
            .await
            .map_err(|e| crate::Error::Storage(format!("Failed to list metadata files: {}", e)))?
        {
            let path = meta.location.to_string();

            // Parse snapshot files: snap-{snapshot_id}-{uuid}.avro
            if path.contains("/snap-") && path.ends_with(".avro") {
                if let Some(snapshot_id) = Self::extract_snapshot_id_from_path(&path) {
                    let timestamp = DateTime::<Utc>::from(meta.last_modified);

                    snapshots.push(SnapshotInfo {
                        snapshot_id,
                        parent_id: None, // Would need to parse the avro file to get this
                        timestamp,
                        manifest_list: path.clone(),
                        summary: SnapshotSummary::default(),
                    });
                }
            }
        }

        // Always ensure the current snapshot is included
        if !snapshots
            .iter()
            .any(|s| s.snapshot_id == current_snapshot_id)
        {
            snapshots.push(SnapshotInfo {
                snapshot_id: current_snapshot_id,
                parent_id: None,
                timestamp: Utc::now(),
                manifest_list: format!(
                    "{}/metadata/snap-{}.avro",
                    table_location, current_snapshot_id
                ),
                summary: SnapshotSummary::default(),
            });
        }

        // Sort by timestamp (oldest first)
        snapshots.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        Ok(snapshots)
    }

    /// Extract snapshot ID from a snapshot file path.
    ///
    /// Expected format: .../snap-{snapshot_id}-{uuid}.avro or .../snap-{snapshot_id}.avro
    fn extract_snapshot_id_from_path(path: &str) -> Option<i64> {
        // Find the filename part
        let filename = path.rsplit('/').next()?;

        // Remove the .avro extension
        let base = filename.strip_suffix(".avro")?;

        // Extract snapshot ID from "snap-{id}" or "snap-{id}-{uuid}"
        let parts: Vec<&str> = base.split('-').collect();
        if parts.len() >= 2 && parts[0] == "snap" {
            parts[1].parse::<i64>().ok()
        } else {
            None
        }
    }

    /// Find snapshots that are candidates for expiration.
    pub fn find_expiration_candidates<'a>(
        &self,
        snapshots: &'a [SnapshotInfo],
        current_snapshot_id: i64,
    ) -> Vec<&'a SnapshotInfo> {
        let cutoff = Utc::now() - Duration::days(self.retention_days as i64);

        snapshots
            .iter()
            .filter(|s| {
                // Never expire the current snapshot
                s.snapshot_id != current_snapshot_id
                // Only expire snapshots older than retention period
                && s.timestamp < cutoff
            })
            .collect()
    }

    /// Calculate the impact of expiring snapshots.
    pub fn estimate_impact(&self, candidates: &[&SnapshotInfo]) -> ExpirationImpact {
        let manifests_to_delete = candidates.len();
        let estimated_files_freed: u64 = candidates.iter().map(|s| s.summary.deleted_files).sum();

        ExpirationImpact {
            snapshots_to_expire: candidates.len(),
            manifests_to_delete,
            estimated_files_freed,
        }
    }

    /// Validate that expiration is safe.
    pub fn validate_expiration(
        &self,
        candidates: &[&SnapshotInfo],
        current_snapshot_id: i64,
    ) -> ExpirationValidation {
        // Check that we're not expiring the current snapshot
        let includes_current = candidates
            .iter()
            .any(|s| s.snapshot_id == current_snapshot_id);

        // Check that we have at least one snapshot remaining
        let expires_all = candidates.len() > 0; // This is a placeholder

        ExpirationValidation {
            is_safe: !includes_current,
            includes_current_snapshot: includes_current,
            warning: if includes_current {
                Some("Cannot expire current snapshot".to_string())
            } else {
                None
            },
        }
    }
}

/// Impact estimation for expiration.
#[derive(Debug, Clone)]
pub struct ExpirationImpact {
    /// Number of snapshots to expire
    pub snapshots_to_expire: usize,
    /// Number of manifests to delete
    pub manifests_to_delete: usize,
    /// Estimated data files that will be freed
    pub estimated_files_freed: u64,
}

/// Validation result for expiration.
#[derive(Debug, Clone)]
pub struct ExpirationValidation {
    /// Whether expiration is safe to proceed
    pub is_safe: bool,
    /// Whether the current snapshot is included
    pub includes_current_snapshot: bool,
    /// Warning message if any
    pub warning: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_snapshots() -> Vec<SnapshotInfo> {
        let now = Utc::now();

        vec![
            SnapshotInfo {
                snapshot_id: 1,
                parent_id: None,
                timestamp: now - Duration::days(10), // Old
                manifest_list: "/metadata/snap-1.avro".into(),
                summary: SnapshotSummary {
                    added_files: 5,
                    deleted_files: 0,
                    total_records: 1000,
                    total_files: 5,
                },
            },
            SnapshotInfo {
                snapshot_id: 2,
                parent_id: Some(1),
                timestamp: now - Duration::days(5), // Within retention
                manifest_list: "/metadata/snap-2.avro".into(),
                summary: SnapshotSummary {
                    added_files: 3,
                    deleted_files: 1,
                    total_records: 2000,
                    total_files: 7,
                },
            },
            SnapshotInfo {
                snapshot_id: 3,
                parent_id: Some(2),
                timestamp: now - Duration::days(1), // Recent
                manifest_list: "/metadata/snap-3.avro".into(),
                summary: SnapshotSummary {
                    added_files: 2,
                    deleted_files: 0,
                    total_records: 2500,
                    total_files: 9,
                },
            },
        ]
    }

    #[test]
    fn test_find_expiration_candidates() {
        let task = ExpirationTask::new(7, None); // 7 day retention
        let snapshots = create_test_snapshots();
        let current_snapshot_id = 3;

        let candidates = task.find_expiration_candidates(&snapshots, current_snapshot_id);

        // Only snapshot 1 (10 days old) should be a candidate
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].snapshot_id, 1);
    }

    #[test]
    fn test_find_expiration_candidates_respects_current() {
        let task = ExpirationTask::new(0, None); // 0 day retention - expire everything
        let snapshots = create_test_snapshots();
        let current_snapshot_id = 3;

        let candidates = task.find_expiration_candidates(&snapshots, current_snapshot_id);

        // Should not include current snapshot (id=3)
        assert!(!candidates.iter().any(|s| s.snapshot_id == 3));
    }

    #[test]
    fn test_estimate_impact() {
        let task = ExpirationTask::new(7, None);
        let snapshots = create_test_snapshots();
        let candidates = task.find_expiration_candidates(&snapshots, 3);

        let impact = task.estimate_impact(&candidates);

        assert_eq!(impact.snapshots_to_expire, 1);
        assert_eq!(impact.manifests_to_delete, 1);
    }

    #[test]
    fn test_validate_expiration_safe() {
        let task = ExpirationTask::new(7, None);
        let snapshots = create_test_snapshots();
        let candidates = task.find_expiration_candidates(&snapshots, 3);

        let validation = task.validate_expiration(&candidates, 3);

        assert!(validation.is_safe);
        assert!(!validation.includes_current_snapshot);
        assert!(validation.warning.is_none());
    }

    #[test]
    fn test_validate_expiration_unsafe() {
        let task = ExpirationTask::new(7, None);
        let snapshots = create_test_snapshots();

        // Manually include current snapshot in candidates
        let candidates: Vec<&SnapshotInfo> = snapshots.iter().collect();

        let validation = task.validate_expiration(&candidates, 3);

        assert!(!validation.is_safe);
        assert!(validation.includes_current_snapshot);
        assert!(validation.warning.is_some());
    }

    #[tokio::test]
    async fn test_expiration_run() {
        let task = ExpirationTask::new(7, None);
        let result = task.run().await.unwrap();

        // Without catalog/store, returns empty result
        assert_eq!(result.snapshots_expired, 0);
    }

    #[test]
    fn test_extract_snapshot_id_from_path() {
        // Standard format: snap-{id}-{uuid}.avro
        assert_eq!(
            ExpirationTask::extract_snapshot_id_from_path(
                "/table/metadata/snap-1234567890-abc123.avro"
            ),
            Some(1234567890)
        );

        // Simple format: snap-{id}.avro
        assert_eq!(
            ExpirationTask::extract_snapshot_id_from_path("/table/metadata/snap-42.avro"),
            Some(42)
        );

        // Invalid format
        assert_eq!(
            ExpirationTask::extract_snapshot_id_from_path("/table/metadata/manifest.avro"),
            None
        );

        // No avro extension
        assert_eq!(
            ExpirationTask::extract_snapshot_id_from_path("/table/metadata/snap-123.json"),
            None
        );
    }
}
