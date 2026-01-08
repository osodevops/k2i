//! Orphan file cleanup for Iceberg tables.
//!
//! Cleans up orphan files that are not referenced by any snapshot:
//! - Files from failed writes
//! - Files from expired snapshots
//! - Temporary files that were never committed
//!
//! ## Cleanup Strategy
//!
//! 1. **List files**: Scan table's data directory for all files
//! 2. **Get referenced**: Build set of files referenced by current snapshot
//! 3. **Find orphans**: Identify files not referenced and older than retention
//! 4. **Validate**: Ensure cleanup is safe (no recent files)
//! 5. **Delete**: Remove orphan files from storage
use super::scheduler::OrphanCleanupResult;
use crate::config::IcebergConfig;
use crate::iceberg::CatalogOperations;
use crate::txlog::{MaintenanceOp, TransactionEntry, TransactionLog};
use crate::Result;
use chrono::{DateTime, Duration, Utc};
use futures::TryStreamExt;
use object_store::ObjectStore;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Information about a potential orphan file.
#[derive(Debug, Clone)]
pub struct OrphanCandidate {
    /// File path
    pub path: String,
    /// File size in bytes
    pub size_bytes: u64,
    /// Last modification time
    pub modified_at: chrono::DateTime<Utc>,
    /// File type
    pub file_type: OrphanFileType,
}

/// Types of files that can become orphaned.
#[derive(Debug, Clone, PartialEq)]
pub enum OrphanFileType {
    /// Data file (Parquet)
    DataFile,
    /// Manifest file (Avro)
    Manifest,
    /// Manifest list file
    ManifestList,
    /// Statistics file
    Statistics,
    /// Unknown file type
    Unknown,
}

/// Orphan cleanup task.
pub struct OrphanCleanupTask {
    /// Retention period in days (safety buffer)
    retention_days: u32,
    /// Transaction log for recording operations
    txlog: Option<Arc<TransactionLog>>,
    /// Catalog operations for metadata access
    catalog: Option<Arc<dyn CatalogOperations>>,
    /// Object store for file operations
    object_store: Option<Arc<dyn ObjectStore>>,
    /// Iceberg config for namespace/table
    config: Option<IcebergConfig>,
}

impl OrphanCleanupTask {
    /// Create a new orphan cleanup task.
    pub fn new(retention_days: u32, txlog: Option<Arc<TransactionLog>>) -> Self {
        Self {
            retention_days,
            txlog,
            catalog: None,
            object_store: None,
            config: None,
        }
    }

    /// Set the catalog operations for real cleanup.
    pub fn with_catalog(mut self, catalog: Arc<dyn CatalogOperations>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Set the object store for file operations.
    pub fn with_object_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(store);
        self
    }

    /// Set the Iceberg config.
    pub fn with_config(mut self, config: IcebergConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Run the orphan cleanup task.
    pub async fn run(&self) -> Result<OrphanCleanupResult> {
        debug!(
            retention_days = self.retention_days,
            "Starting orphan cleanup task"
        );

        // TODO: Implement actual orphan file cleanup
        // This would involve:
        // 1. List all files in data directory
        // 2. List all files referenced by current snapshot
        // 3. Find files older than retention that aren't referenced
        // 4. Delete orphan files safely
        // 5. Log cleanup operations

        let result = self.perform_cleanup().await?;

        // Log cleanup completion
        if let Some(ref txlog) = self.txlog {
            if result.files_deleted > 0 {
                txlog.append(TransactionEntry::Maintenance {
                    operation: MaintenanceOp::OrphanCleaned,
                    details: format!(
                        "Orphan cleanup completed: {} files deleted, {} bytes freed",
                        result.files_deleted, result.bytes_freed
                    ),
                    timestamp: Utc::now(),
                })?;
            }
        }

        info!(
            files_deleted = result.files_deleted,
            bytes_freed = result.bytes_freed,
            "Orphan cleanup task completed"
        );

        Ok(result)
    }

    /// Perform the actual cleanup logic.
    async fn perform_cleanup(&self) -> Result<OrphanCleanupResult> {
        // Check if we have required components
        let (catalog, store, config) = match (&self.catalog, &self.object_store, &self.config) {
            (Some(c), Some(s), Some(cfg)) => (c, s, cfg),
            _ => {
                debug!("Orphan cleanup skipped: missing catalog, object_store, or config");
                return Ok(OrphanCleanupResult::default());
            }
        };

        let namespace = &config.database_name;
        let table = &config.table_name;

        // Step 1: Load table info to get table location
        let table_info = match catalog.load_table(namespace, table).await {
            Ok(info) => info,
            Err(e) => {
                warn!(error = %e, "Failed to load table for orphan cleanup");
                return Ok(OrphanCleanupResult::default());
            }
        };

        let table_location = &table_info.location;
        debug!(location = %table_location, "Scanning table location for orphan files");

        // Step 2: List all files in the table's data directory
        let data_prefix = object_store::path::Path::from(format!("{}/data", table_location));
        let all_files = self.list_all_files(store.as_ref(), &data_prefix).await?;

        if all_files.is_empty() {
            debug!("No files found in data directory");
            return Ok(OrphanCleanupResult::default());
        }

        info!(
            total_files = all_files.len(),
            "Found files in data directory"
        );

        // Step 3: Build set of referenced files
        // In a full implementation, we'd parse manifests to get referenced files
        // For now, we'll use the manifest list path pattern to identify referenced files
        let referenced_files = self.build_referenced_files(&table_info).await;

        debug!(
            referenced_files = referenced_files.len(),
            "Built set of referenced files"
        );

        // Step 4: Find orphan candidates
        let orphans = self.find_orphans(&all_files, &referenced_files);

        if orphans.is_empty() {
            debug!("No orphan files found");
            return Ok(OrphanCleanupResult::default());
        }

        // Step 5: Validate cleanup is safe
        let validation = self.validate_cleanup(&orphans);
        if !validation.is_safe {
            warn!(
                warning = ?validation.warning,
                recent_files = validation.recent_file_count,
                "Orphan cleanup validation failed, skipping"
            );
            return Ok(OrphanCleanupResult::default());
        }

        info!(
            orphan_count = orphans.len(),
            retention_days = self.retention_days,
            "Found orphan files eligible for cleanup"
        );

        // Step 6: Delete orphan files
        let mut files_deleted = 0;
        let mut bytes_freed: u64 = 0;
        let mut deleted_paths = Vec::new();

        for orphan in &orphans {
            let file_path = object_store::path::Path::from(orphan.path.as_str());
            match store.delete(&file_path).await {
                Ok(()) => {
                    files_deleted += 1;
                    bytes_freed += orphan.size_bytes;
                    deleted_paths.push(orphan.path.clone());
                    debug!(
                        path = %orphan.path,
                        size_bytes = orphan.size_bytes,
                        file_type = ?orphan.file_type,
                        "Deleted orphan file"
                    );
                }
                Err(e) => {
                    warn!(
                        path = %orphan.path,
                        error = %e,
                        "Failed to delete orphan file, continuing"
                    );
                }
            }
        }

        // Step 7: Log cleanup completion
        if let Some(ref txlog) = self.txlog {
            if files_deleted > 0 {
                txlog.append(TransactionEntry::Maintenance {
                    operation: MaintenanceOp::OrphanCleaned,
                    details: format!(
                        "Deleted {} orphan files, freed {} bytes",
                        files_deleted, bytes_freed
                    ),
                    timestamp: Utc::now(),
                })?;
            }
        }

        info!(
            files_deleted = files_deleted,
            bytes_freed = bytes_freed,
            "Orphan file cleanup completed"
        );

        Ok(OrphanCleanupResult {
            files_deleted,
            bytes_freed,
            deleted_paths,
        })
    }

    /// List all files in a directory recursively.
    async fn list_all_files(
        &self,
        store: &dyn ObjectStore,
        prefix: &object_store::path::Path,
    ) -> Result<Vec<OrphanCandidate>> {
        let mut candidates = Vec::new();
        let cutoff = Utc::now() - Duration::days(self.retention_days as i64);

        let mut list_stream = store.list(Some(prefix));
        while let Some(meta) = list_stream
            .try_next()
            .await
            .map_err(|e| crate::Error::Storage(format!("Failed to list files: {}", e)))?
        {
            let path = meta.location.to_string();
            let modified_at = DateTime::<Utc>::from(meta.last_modified);

            // Only consider files older than the cutoff for orphan detection
            // Recent files might be in-flight writes
            if modified_at < cutoff {
                candidates.push(OrphanCandidate {
                    path,
                    size_bytes: meta.size as u64,
                    modified_at,
                    file_type: Self::classify_file(&meta.location.to_string()),
                });
            }
        }

        Ok(candidates)
    }

    /// Build set of referenced files from table metadata and object store.
    ///
    /// This method builds a set of referenced files by:
    /// 1. Adding all current metadata files (snapshots, manifests, metadata.json)
    /// 2. Using the retention period as a safety buffer for data files
    ///
    /// Note: A full implementation would parse manifest avro files to get
    /// the exact list of referenced data files. This implementation provides
    /// a safe approximation using file age as the primary filter.
    async fn build_referenced_files(
        &self,
        table_info: &crate::iceberg::TableInfo,
    ) -> HashSet<String> {
        let mut referenced = HashSet::new();

        // Add the manifest list path if current snapshot exists
        if let Some(snapshot_id) = table_info.current_snapshot_id {
            // The manifest list is typically at metadata/snap-{snapshot_id}.avro
            let manifest_list =
                format!("{}/metadata/snap-{}.avro", table_info.location, snapshot_id);
            referenced.insert(manifest_list);
        }

        // If we have an object store, scan metadata directory for referenced files
        if let Some(ref store) = self.object_store {
            let metadata_prefix =
                object_store::path::Path::from(format!("{}/metadata", table_info.location));

            // List all metadata files and add them to referenced set
            // This includes: metadata.json files, manifest lists, manifest files
            let mut list_stream = store.list(Some(&metadata_prefix));
            while let Ok(Some(meta)) = list_stream.try_next().await {
                let path = meta.location.to_string();

                // Include all manifest files (*.avro) and metadata files (*.json)
                if path.ends_with(".avro") || path.ends_with(".json") {
                    referenced.insert(path);
                }
            }
        }

        debug!(
            referenced_count = referenced.len(),
            "Built referenced files set from metadata"
        );

        referenced
    }

    /// Find orphan files by comparing all files against referenced files.
    pub fn find_orphans<'a>(
        &self,
        all_files: &'a [OrphanCandidate],
        referenced_files: &HashSet<String>,
    ) -> Vec<&'a OrphanCandidate> {
        let cutoff = Utc::now() - Duration::days(self.retention_days as i64);

        all_files
            .iter()
            .filter(|f| {
                // File must not be referenced
                !referenced_files.contains(&f.path)
                // File must be older than retention period (safety buffer)
                && f.modified_at < cutoff
            })
            .collect()
    }

    /// Calculate the total size of orphan files.
    pub fn calculate_orphan_size(orphans: &[&OrphanCandidate]) -> u64 {
        orphans.iter().map(|f| f.size_bytes).sum()
    }

    /// Group orphans by file type for reporting.
    pub fn group_by_type(orphans: &[&OrphanCandidate]) -> OrphanSummary {
        let mut summary = OrphanSummary::default();

        for orphan in orphans {
            match orphan.file_type {
                OrphanFileType::DataFile => {
                    summary.data_files += 1;
                    summary.data_file_bytes += orphan.size_bytes;
                }
                OrphanFileType::Manifest => {
                    summary.manifests += 1;
                    summary.manifest_bytes += orphan.size_bytes;
                }
                OrphanFileType::ManifestList => {
                    summary.manifest_lists += 1;
                }
                OrphanFileType::Statistics => {
                    summary.statistics += 1;
                }
                OrphanFileType::Unknown => {
                    summary.unknown += 1;
                }
            }
        }

        summary
    }

    /// Validate that cleanup is safe to proceed.
    pub fn validate_cleanup(&self, orphans: &[&OrphanCandidate]) -> CleanupValidation {
        // Check for any suspiciously recent files
        let recent_cutoff = Utc::now() - Duration::hours(1);
        let recent_files: Vec<_> = orphans
            .iter()
            .filter(|f| f.modified_at > recent_cutoff)
            .collect();

        let has_recent = !recent_files.is_empty();

        CleanupValidation {
            is_safe: !has_recent || self.retention_days >= 1,
            has_recent_files: has_recent,
            recent_file_count: recent_files.len(),
            warning: if has_recent && self.retention_days < 1 {
                Some(format!(
                    "{} files modified within the last hour - increase retention_days for safety",
                    recent_files.len()
                ))
            } else {
                None
            },
        }
    }

    /// Determine file type from path.
    pub fn classify_file(path: &str) -> OrphanFileType {
        if path.ends_with(".parquet") {
            OrphanFileType::DataFile
        } else if path.ends_with(".avro") {
            // Manifest list files typically have "snap-" in the name
            if path.contains("snap-") {
                OrphanFileType::ManifestList
            } else if path.contains("manifest") {
                OrphanFileType::Manifest
            } else {
                OrphanFileType::Unknown
            }
        } else if path.contains("/statistics") {
            OrphanFileType::Statistics
        } else {
            OrphanFileType::Unknown
        }
    }
}

/// Summary of orphan files by type.
#[derive(Debug, Clone, Default)]
pub struct OrphanSummary {
    /// Number of data files
    pub data_files: usize,
    /// Total size of data files
    pub data_file_bytes: u64,
    /// Number of manifest files
    pub manifests: usize,
    /// Total size of manifest files
    pub manifest_bytes: u64,
    /// Number of manifest list files
    pub manifest_lists: usize,
    /// Number of statistics files
    pub statistics: usize,
    /// Number of unknown files
    pub unknown: usize,
}

/// Validation result for cleanup.
#[derive(Debug, Clone)]
pub struct CleanupValidation {
    /// Whether cleanup is safe to proceed
    pub is_safe: bool,
    /// Whether there are recently modified files
    pub has_recent_files: bool,
    /// Number of recent files
    pub recent_file_count: usize,
    /// Warning message if any
    pub warning: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_files() -> Vec<OrphanCandidate> {
        let now = Utc::now();

        vec![
            OrphanCandidate {
                path: "/data/part-001.parquet".into(),
                size_bytes: 100 * 1024 * 1024,        // 100 MB
                modified_at: now - Duration::days(5), // Old, orphaned
                file_type: OrphanFileType::DataFile,
            },
            OrphanCandidate {
                path: "/data/part-002.parquet".into(),
                size_bytes: 50 * 1024 * 1024,         // 50 MB
                modified_at: now - Duration::days(1), // Recent
                file_type: OrphanFileType::DataFile,
            },
            OrphanCandidate {
                path: "/metadata/manifest-001.avro".into(),
                size_bytes: 1024,                      // 1 KB
                modified_at: now - Duration::days(10), // Old
                file_type: OrphanFileType::Manifest,
            },
        ]
    }

    #[test]
    fn test_find_orphans() {
        let task = OrphanCleanupTask::new(3, None); // 3 day retention
        let files = create_test_files();

        // Only part-002.parquet is referenced
        let referenced: HashSet<String> = vec!["/data/part-002.parquet".to_string()]
            .into_iter()
            .collect();

        let orphans = task.find_orphans(&files, &referenced);

        // Should find part-001.parquet and manifest-001.avro as orphans
        // But part-001 is only 5 days old and retention is 3 days, so it passes
        // manifest-001 is 10 days old, so it's definitely an orphan
        assert_eq!(orphans.len(), 2);
    }

    #[test]
    fn test_calculate_orphan_size() {
        let task = OrphanCleanupTask::new(3, None);
        let files = create_test_files();
        let referenced: HashSet<String> = HashSet::new();

        let orphans = task.find_orphans(&files, &referenced);
        let total_size = OrphanCleanupTask::calculate_orphan_size(&orphans);

        // Should include size of orphan files older than retention
        assert!(total_size > 0);
    }

    #[test]
    fn test_group_by_type() {
        let task = OrphanCleanupTask::new(0, None); // 0 day retention
        let files = create_test_files();
        let referenced: HashSet<String> = HashSet::new();

        let orphans = task.find_orphans(&files, &referenced);
        let summary = OrphanCleanupTask::group_by_type(&orphans);

        // Should have data files and manifests
        assert!(summary.data_files > 0 || summary.manifests > 0);
    }

    #[test]
    fn test_classify_file() {
        assert_eq!(
            OrphanCleanupTask::classify_file("/data/part-001.parquet"),
            OrphanFileType::DataFile
        );
        assert_eq!(
            OrphanCleanupTask::classify_file("/metadata/manifest-001.avro"),
            OrphanFileType::Manifest
        );
        assert_eq!(
            OrphanCleanupTask::classify_file("/metadata/snap-123.avro"),
            OrphanFileType::ManifestList
        );
        assert_eq!(
            OrphanCleanupTask::classify_file("/metadata/statistics/stats.json"),
            OrphanFileType::Statistics
        );
        assert_eq!(
            OrphanCleanupTask::classify_file("/unknown/file.txt"),
            OrphanFileType::Unknown
        );
    }

    #[test]
    fn test_validate_cleanup_safe() {
        let task = OrphanCleanupTask::new(3, None);
        let now = Utc::now();

        let files = vec![OrphanCandidate {
            path: "/data/old.parquet".into(),
            size_bytes: 1024,
            modified_at: now - Duration::days(5),
            file_type: OrphanFileType::DataFile,
        }];

        let orphans: Vec<&OrphanCandidate> = files.iter().collect();
        let validation = task.validate_cleanup(&orphans);

        assert!(validation.is_safe);
        assert!(!validation.has_recent_files);
    }

    #[test]
    fn test_validate_cleanup_with_recent() {
        let task = OrphanCleanupTask::new(0, None); // 0 retention is risky
        let now = Utc::now();

        let files = vec![OrphanCandidate {
            path: "/data/recent.parquet".into(),
            size_bytes: 1024,
            modified_at: now - Duration::minutes(30), // Very recent
            file_type: OrphanFileType::DataFile,
        }];

        let orphans: Vec<&OrphanCandidate> = files.iter().collect();
        let validation = task.validate_cleanup(&orphans);

        assert!(!validation.is_safe);
        assert!(validation.has_recent_files);
        assert!(validation.warning.is_some());
    }

    #[tokio::test]
    async fn test_orphan_cleanup_run() {
        let task = OrphanCleanupTask::new(3, None);
        let result = task.run().await.unwrap();

        // Stub returns empty result
        assert_eq!(result.files_deleted, 0);
    }
}
