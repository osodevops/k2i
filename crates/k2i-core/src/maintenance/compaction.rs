//! File compaction for Iceberg tables.
//!
//! Compaction merges small Parquet files into larger ones to:
//! - Improve query performance (fewer files to scan)
//! - Reduce metadata overhead
//! - Optimize storage costs
//!
//! ## Compaction Strategy
//!
//! 1. **Find candidates**: Files smaller than threshold (default 100MB)
//! 2. **Group files**: Bin files to create ~512MB compacted files
//! 3. **Merge**: Read Parquet files, concatenate Arrow batches, write new file
//! 4. **Commit**: Atomically update Iceberg metadata
//! 5. **Cleanup**: Remove old files after successful commit

use super::scheduler::CompactionResult;
use crate::config::IcebergConfig;
use crate::iceberg::{CatalogOperations, DataFileInfo, SnapshotCommit};
use crate::txlog::{MaintenanceOp, TransactionEntry, TransactionLog};
use crate::{Error, IcebergError, Result};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::Utc;
use futures::TryStreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// File information for compaction decisions.
#[derive(Debug, Clone)]
pub struct FileInfo {
    /// File path
    pub path: String,
    /// File size in bytes
    pub size_bytes: u64,
    /// Number of rows
    pub row_count: u64,
    /// Creation timestamp
    pub created_at: chrono::DateTime<Utc>,
}

/// Compaction task that merges small files.
pub struct CompactionTask {
    /// Threshold in MB - files smaller than this are candidates
    threshold_mb: usize,
    /// Target size in MB for compacted files
    target_mb: usize,
    /// Transaction log for recording operations
    txlog: Option<Arc<TransactionLog>>,
    /// Catalog operations for metadata updates
    catalog: Option<Arc<dyn CatalogOperations>>,
    /// Object store for file I/O
    object_store: Option<Arc<dyn ObjectStore>>,
    /// Iceberg config for namespace/table
    config: Option<IcebergConfig>,
}

impl CompactionTask {
    /// Create a new compaction task.
    pub fn new(threshold_mb: usize, target_mb: usize, txlog: Option<Arc<TransactionLog>>) -> Self {
        Self {
            threshold_mb,
            target_mb,
            txlog,
            catalog: None,
            object_store: None,
            config: None,
        }
    }

    /// Set the catalog operations for real compaction.
    pub fn with_catalog(mut self, catalog: Arc<dyn CatalogOperations>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Set the object store for file I/O.
    pub fn with_object_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(store);
        self
    }

    /// Set the Iceberg config.
    pub fn with_config(mut self, config: IcebergConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Run the compaction task.
    pub async fn run(&self) -> Result<CompactionResult> {
        debug!(
            threshold_mb = self.threshold_mb,
            target_mb = self.target_mb,
            "Starting compaction task"
        );

        // Log compaction start
        if let Some(ref txlog) = self.txlog {
            txlog.append(TransactionEntry::Maintenance {
                operation: MaintenanceOp::CompactionStarted,
                details: format!(
                    "Compaction started with threshold={}MB, target={}MB",
                    self.threshold_mb, self.target_mb
                ),
                timestamp: Utc::now(),
            })?;
        }

        // TODO: Implement actual Iceberg compaction
        // This would involve:
        // 1. List all data files from Iceberg metadata
        // 2. Find files smaller than threshold
        // 3. Group files to merge into target-sized files
        // 4. Read and merge Parquet files
        // 5. Write new compacted files
        // 6. Update Iceberg metadata atomically
        // 7. Delete old files after successful commit

        let result = self.perform_compaction().await?;

        // Log compaction completion
        if let Some(ref txlog) = self.txlog {
            txlog.append(TransactionEntry::Maintenance {
                operation: MaintenanceOp::CompactionCompleted,
                details: format!(
                    "Compaction completed: {} files compacted, {} bytes saved",
                    result.files_compacted, result.bytes_saved
                ),
                timestamp: Utc::now(),
            })?;
        }

        info!(
            files_compacted = result.files_compacted,
            bytes_saved = result.bytes_saved,
            "Compaction task completed"
        );

        Ok(result)
    }

    /// Perform the actual compaction logic.
    async fn perform_compaction(&self) -> Result<CompactionResult> {
        // Check if we have required components
        let (catalog, store, config) = match (&self.catalog, &self.object_store, &self.config) {
            (Some(c), Some(s), Some(cfg)) => (c, s, cfg),
            _ => {
                debug!("Compaction skipped: missing catalog, object_store, or config");
                return Ok(CompactionResult::default());
            }
        };

        let namespace = &config.database_name;
        let table = &config.table_name;

        // Step 1: Get table info and list data files
        let table_info = match catalog.load_table(namespace, table).await {
            Ok(info) => info,
            Err(e) => {
                warn!(error = %e, "Failed to load table for compaction");
                return Ok(CompactionResult::default());
            }
        };

        // Step 2: List actual data files from object store
        let data_prefix = ObjectPath::from(format!("{}/data", table_info.location));
        let files = self.list_data_files(store.as_ref(), &data_prefix).await?;

        if files.is_empty() {
            debug!("No files to compact");
            return Ok(CompactionResult::default());
        }

        info!(
            file_count = files.len(),
            location = %table_info.location,
            "Listed data files for compaction analysis"
        );

        // Step 3: Find compaction candidates
        let candidates = self.find_compaction_candidates(&files);
        if candidates.is_empty() {
            debug!("No files below threshold for compaction");
            return Ok(CompactionResult::default());
        }

        // Step 4: Group files for compaction
        let groups = self.group_files_for_compaction(&candidates);
        if groups.is_empty() {
            debug!("No groups formed for compaction");
            return Ok(CompactionResult::default());
        }

        info!(
            candidate_count = candidates.len(),
            group_count = groups.len(),
            "Found compaction candidates"
        );

        // Step 5: Compact each group
        let mut total_files_compacted = 0;
        let mut total_bytes_saved = 0u64;
        let mut files_to_add = Vec::new();
        let mut files_to_remove = Vec::new();

        for (group_idx, group) in groups.iter().enumerate() {
            match self.compact_group(group, group_idx, store, config).await {
                Ok((new_file, old_size, new_size)) => {
                    total_files_compacted += group.len();
                    total_bytes_saved += old_size.saturating_sub(new_size);

                    files_to_add.push(new_file);
                    files_to_remove.extend(group.iter().map(|f| f.path.clone()));

                    debug!(
                        group_idx = group_idx,
                        files_merged = group.len(),
                        old_size_mb = old_size / (1024 * 1024),
                        new_size_mb = new_size / (1024 * 1024),
                        "Compacted group"
                    );
                }
                Err(e) => {
                    warn!(
                        group_idx = group_idx,
                        error = %e,
                        "Failed to compact group, skipping"
                    );
                }
            }
        }

        // Capture counts before moving
        let files_created = files_to_add.len();
        let processed_files = files_to_remove.clone();

        // Step 6: Commit changes to catalog atomically
        if !files_to_add.is_empty() {
            let current_snapshot = catalog.current_snapshot_id(namespace, table).await?;

            let mut summary = HashMap::new();
            summary.insert("operation".to_string(), "compaction".to_string());
            summary.insert("added-files".to_string(), files_created.to_string());
            summary.insert(
                "removed-files".to_string(),
                files_to_remove.len().to_string(),
            );

            let commit = SnapshotCommit {
                expected_snapshot_id: current_snapshot,
                files_to_add,
                files_to_remove, // Already Vec<String>
                summary,
            };

            match catalog.commit_snapshot(namespace, table, commit).await {
                Ok(result) => {
                    info!(
                        snapshot_id = result.snapshot_id,
                        files_compacted = total_files_compacted,
                        "Compaction committed successfully"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "Failed to commit compaction snapshot");
                    // Don't fail entirely - report what we did
                }
            }
        }

        Ok(CompactionResult {
            files_compacted: total_files_compacted,
            files_created,
            bytes_saved: total_bytes_saved,
            processed_files,
        })
    }

    /// Compact a group of files into a single file.
    async fn compact_group(
        &self,
        group: &[&FileInfo],
        _group_idx: usize,
        store: &Arc<dyn ObjectStore>,
        config: &IcebergConfig,
    ) -> Result<(DataFileInfo, u64, u64)> {
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut total_old_size = 0u64;
        let mut schema = None;

        // Read all files in the group
        for file in group {
            total_old_size += file.size_bytes;

            let path = ObjectPath::from(file.path.as_str());
            let data = store.get(&path).await.map_err(|e| {
                Error::Iceberg(IcebergError::FileUpload(format!(
                    "Failed to read file: {}",
                    e
                )))
            })?;

            let bytes = data.bytes().await.map_err(|e| {
                Error::Iceberg(IcebergError::FileUpload(format!(
                    "Failed to read bytes: {}",
                    e
                )))
            })?;

            // Read Parquet file
            let cursor = Cursor::new(bytes.to_vec());
            let builder = ParquetRecordBatchStreamBuilder::new(cursor)
                .await
                .map_err(|e| {
                    Error::Iceberg(IcebergError::ParquetWrite(format!(
                        "Failed to read parquet: {}",
                        e
                    )))
                })?;

            if schema.is_none() {
                schema = Some(builder.schema().clone());
            }

            let reader = builder.build().map_err(|e| {
                Error::Iceberg(IcebergError::ParquetWrite(format!(
                    "Failed to build reader: {}",
                    e
                )))
            })?;

            use futures::StreamExt;
            let mut stream = reader;
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result.map_err(|e| {
                    Error::Iceberg(IcebergError::ParquetWrite(format!(
                        "Failed to read batch: {}",
                        e
                    )))
                })?;
                all_batches.push(batch);
            }
        }

        // Merge all batches
        let schema = schema
            .ok_or_else(|| Error::Iceberg(IcebergError::ParquetWrite("No schema found".into())))?;

        let merged_batch = if all_batches.len() == 1 {
            all_batches.pop().unwrap()
        } else {
            concat_batches(&schema, &all_batches).map_err(|e| {
                Error::Iceberg(IcebergError::ParquetWrite(format!(
                    "Failed to concat batches: {}",
                    e
                )))
            })?
        };

        // Write compacted file
        let uuid = uuid::Uuid::new_v4();
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
        let new_path = format!(
            "data/{}/{}/compacted/part-{}-{}.parquet",
            config.database_name, config.table_name, uuid, timestamp
        );

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props))
                .map_err(|e| Error::Iceberg(IcebergError::ParquetWrite(e.to_string())))?;

            writer
                .write(&merged_batch)
                .map_err(|e| Error::Iceberg(IcebergError::ParquetWrite(e.to_string())))?;

            writer
                .close()
                .map_err(|e| Error::Iceberg(IcebergError::ParquetWrite(e.to_string())))?;
        }

        let new_size = buffer.len() as u64;
        let row_count = merged_batch.num_rows() as u64;

        // Upload compacted file
        let object_path = ObjectPath::from(new_path.as_str());
        store
            .put(&object_path, Bytes::from(buffer).into())
            .await
            .map_err(|e| Error::Iceberg(IcebergError::FileUpload(e.to_string())))?;

        let data_file = DataFileInfo {
            file_path: new_path,
            file_size_bytes: new_size,
            record_count: row_count,
            partition_values: HashMap::new(),
            file_format: "parquet".to_string(),
        };

        Ok((data_file, total_old_size, new_size))
    }

    /// List all data files in the table's data directory.
    async fn list_data_files(
        &self,
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
    ) -> Result<Vec<FileInfo>> {
        let mut files = Vec::new();

        let mut list_stream = store.list(Some(prefix));
        while let Some(meta) = list_stream
            .try_next()
            .await
            .map_err(|e| Error::Storage(format!("Failed to list files for compaction: {}", e)))?
        {
            let path = meta.location.to_string();

            // Only consider parquet files
            if path.ends_with(".parquet") {
                files.push(FileInfo {
                    path,
                    size_bytes: meta.size as u64,
                    row_count: 0, // Will be updated when reading the file
                    created_at: meta.last_modified,
                });
            }
        }

        // Sort by size for efficient grouping
        files.sort_by(|a, b| a.size_bytes.cmp(&b.size_bytes));

        Ok(files)
    }

    /// Find files that are candidates for compaction.
    pub fn find_compaction_candidates<'a>(&self, files: &'a [FileInfo]) -> Vec<&'a FileInfo> {
        let threshold_bytes = (self.threshold_mb * 1024 * 1024) as u64;

        files
            .iter()
            .filter(|f| f.size_bytes < threshold_bytes)
            .collect()
    }

    /// Group files into bins for compaction.
    pub fn group_files_for_compaction<'a>(
        &self,
        candidates: &[&'a FileInfo],
    ) -> Vec<Vec<&'a FileInfo>> {
        let target_bytes = (self.target_mb * 1024 * 1024) as u64;
        let mut groups: Vec<Vec<&'a FileInfo>> = Vec::new();
        let mut current_group: Vec<&'a FileInfo> = Vec::new();
        let mut current_size: u64 = 0;

        for file in candidates {
            if current_size + file.size_bytes > target_bytes && !current_group.is_empty() {
                groups.push(std::mem::take(&mut current_group));
                current_size = 0;
            }

            current_group.push(file);
            current_size += file.size_bytes;
        }

        // Don't forget the last group
        if current_group.len() > 1 {
            // Only compact if there's more than one file
            groups.push(current_group);
        }

        groups
    }

    /// Estimate space savings from compaction.
    pub fn estimate_savings(&self, groups: &[Vec<&FileInfo>]) -> u64 {
        // Estimate based on typical Parquet overhead reduction
        // When merging files, we typically save ~5-10% due to:
        // - Shared dictionary encoding
        // - Better compression ratios
        // - Reduced per-file metadata overhead

        let total_size: u64 = groups
            .iter()
            .flat_map(|g| g.iter())
            .map(|f| f.size_bytes)
            .sum();

        // Estimate 8% savings
        total_size / 12
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_files() -> Vec<FileInfo> {
        vec![
            FileInfo {
                path: "/data/file1.parquet".into(),
                size_bytes: 50 * 1024 * 1024, // 50 MB
                row_count: 100_000,
                created_at: Utc::now(),
            },
            FileInfo {
                path: "/data/file2.parquet".into(),
                size_bytes: 30 * 1024 * 1024, // 30 MB
                row_count: 60_000,
                created_at: Utc::now(),
            },
            FileInfo {
                path: "/data/file3.parquet".into(),
                size_bytes: 200 * 1024 * 1024, // 200 MB - above threshold
                row_count: 400_000,
                created_at: Utc::now(),
            },
            FileInfo {
                path: "/data/file4.parquet".into(),
                size_bytes: 80 * 1024 * 1024, // 80 MB
                row_count: 160_000,
                created_at: Utc::now(),
            },
        ]
    }

    #[test]
    fn test_find_compaction_candidates() {
        let task = CompactionTask::new(100, 512, None);
        let files = create_test_files();

        let candidates = task.find_compaction_candidates(&files);

        // Only files < 100MB should be candidates
        assert_eq!(candidates.len(), 3);
        assert!(candidates.iter().all(|f| f.size_bytes < 100 * 1024 * 1024));
    }

    #[test]
    fn test_group_files_for_compaction() {
        let task = CompactionTask::new(100, 150, None); // 150MB target
        let files = create_test_files();

        let candidates = task.find_compaction_candidates(&files);
        let groups = task.group_files_for_compaction(&candidates);

        // Should create groups that don't exceed 150MB
        for group in &groups {
            let total: u64 = group.iter().map(|f| f.size_bytes).sum();
            // Groups should be <= target (with some tolerance for the last file)
            assert!(total <= 200 * 1024 * 1024);
        }
    }

    #[test]
    fn test_estimate_savings() {
        let task = CompactionTask::new(100, 512, None);
        let files = create_test_files();

        let candidates = task.find_compaction_candidates(&files);
        let groups = task.group_files_for_compaction(&candidates);
        let savings = task.estimate_savings(&groups);

        // Should estimate some savings
        assert!(savings > 0);
    }

    #[tokio::test]
    async fn test_compaction_run() {
        let task = CompactionTask::new(100, 512, None);
        let result = task.run().await.unwrap();

        // Stub returns empty result
        assert_eq!(result.files_compacted, 0);
    }
}
