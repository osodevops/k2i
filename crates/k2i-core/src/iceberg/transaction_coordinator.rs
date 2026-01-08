//! Transaction coordinator for atomic Iceberg commits.
//!
//! Provides:
//! - Atomic CAS (compare-and-swap) commits to Iceberg
//! - Idempotency tracking to prevent duplicate commits
//! - Conflict resolution with exponential backoff retry
//! - Integration with transaction log for durability

use crate::config::CatalogManagerConfig;
use crate::iceberg::factory::{CatalogOperations, DataFileInfo, SnapshotCommit};
use crate::iceberg::metadata_cache::MetadataCache;
use crate::txlog::{TransactionEntry, TransactionLog};
use crate::{Error, IcebergError, Result};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Configuration for the transaction coordinator.
#[derive(Debug, Clone)]
pub struct TransactionCoordinatorConfig {
    /// Maximum number of retry attempts for CAS conflicts
    pub max_retries: u32,
    /// Base delay for exponential backoff (milliseconds)
    pub base_delay_ms: u64,
    /// Maximum delay for exponential backoff (milliseconds)
    pub max_delay_ms: u64,
    /// Enable idempotency checking
    pub enable_idempotency: bool,
    /// Maximum number of idempotency records to keep in memory
    pub max_idempotency_records: usize,
}

impl Default for TransactionCoordinatorConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 100,
            max_delay_ms: 5000,
            enable_idempotency: true,
            max_idempotency_records: 10000,
        }
    }
}

impl From<&CatalogManagerConfig> for TransactionCoordinatorConfig {
    fn from(config: &CatalogManagerConfig) -> Self {
        Self {
            max_retries: config.max_retries,
            base_delay_ms: config.reconnect_backoff_ms.first().copied().unwrap_or(100),
            max_delay_ms: config.reconnect_backoff_ms.last().copied().unwrap_or(5000),
            ..Default::default()
        }
    }
}

/// Key for idempotency tracking.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct IdempotencyKey {
    /// Topic name
    pub topic: String,
    /// Partition number
    pub partition: i32,
    /// Minimum offset in the range
    pub offset_min: i64,
    /// Maximum offset in the range
    pub offset_max: i64,
}

impl IdempotencyKey {
    /// Create a new idempotency key.
    pub fn new(topic: impl Into<String>, partition: i32, offset_min: i64, offset_max: i64) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset_min,
            offset_max,
        }
    }
}

/// Record of a committed transaction for idempotency.
#[derive(Debug, Clone)]
pub struct IdempotencyRecord {
    /// The idempotency key
    pub key: IdempotencyKey,
    /// Snapshot ID that was committed
    pub snapshot_id: i64,
    /// When the commit was made
    pub committed_at: DateTime<Utc>,
    /// File paths that were added
    pub file_paths: Vec<String>,
}

/// Statistics for the transaction coordinator.
#[derive(Debug, Default)]
pub struct TransactionStats {
    /// Total commits attempted
    pub commits_attempted: AtomicU64,
    /// Total commits succeeded
    pub commits_succeeded: AtomicU64,
    /// Total commits failed
    pub commits_failed: AtomicU64,
    /// Total CAS conflicts encountered
    pub cas_conflicts: AtomicU64,
    /// Total retries performed
    pub retries: AtomicU64,
    /// Total idempotent returns (duplicate request)
    pub idempotent_returns: AtomicU64,
}

impl TransactionStats {
    /// Get the success rate.
    pub fn success_rate(&self) -> f64 {
        let attempted = self.commits_attempted.load(Ordering::Relaxed);
        let succeeded = self.commits_succeeded.load(Ordering::Relaxed);
        if attempted == 0 {
            1.0
        } else {
            succeeded as f64 / attempted as f64
        }
    }
}

/// Transaction coordinator for managing Iceberg commits.
pub struct TransactionCoordinator {
    /// Catalog operations
    catalog: Arc<dyn CatalogOperations>,
    /// Namespace (database)
    namespace: String,
    /// Table name
    table_name: String,
    /// Configuration
    config: TransactionCoordinatorConfig,
    /// Metadata cache
    cache: Arc<MetadataCache>,
    /// Transaction log (optional)
    txlog: Option<Arc<TransactionLog>>,
    /// Idempotency records
    idempotency_records: RwLock<HashMap<IdempotencyKey, IdempotencyRecord>>,
    /// Statistics
    stats: TransactionStats,
}

impl TransactionCoordinator {
    /// Create a new transaction coordinator.
    pub fn new(
        catalog: Arc<dyn CatalogOperations>,
        namespace: String,
        table_name: String,
        config: TransactionCoordinatorConfig,
        cache: Arc<MetadataCache>,
    ) -> Self {
        Self {
            catalog,
            namespace,
            table_name,
            config,
            cache,
            txlog: None,
            idempotency_records: RwLock::new(HashMap::new()),
            stats: TransactionStats::default(),
        }
    }

    /// Set the transaction log for durability.
    pub fn with_txlog(mut self, txlog: Arc<TransactionLog>) -> Self {
        self.txlog = Some(txlog);
        self
    }

    /// Commit a snapshot with files to add.
    ///
    /// This is the main entry point for committing data to Iceberg.
    /// It handles:
    /// 1. Idempotency checking
    /// 2. CAS commit with retry on conflict
    /// 3. Transaction log recording
    /// 4. Cache invalidation
    pub async fn commit_snapshot(
        &self,
        files: Vec<DataFileInfo>,
        kafka_offsets: Option<(String, i32, i64, i64)>, // (topic, partition, min_offset, max_offset)
    ) -> Result<CommitResult> {
        self.stats.commits_attempted.fetch_add(1, Ordering::Relaxed);

        // Check idempotency if enabled and offsets provided
        if self.config.enable_idempotency {
            if let Some((ref topic, partition, offset_min, offset_max)) = kafka_offsets {
                let key = IdempotencyKey::new(topic, partition, offset_min, offset_max);

                if let Some(record) = self.check_idempotency(&key) {
                    self.stats
                        .idempotent_returns
                        .fetch_add(1, Ordering::Relaxed);
                    info!(
                        snapshot_id = record.snapshot_id,
                        topic = %topic,
                        partition = partition,
                        offset_min = offset_min,
                        offset_max = offset_max,
                        "Idempotent return: offset range already committed"
                    );
                    return Ok(CommitResult {
                        snapshot_id: record.snapshot_id,
                        committed_at: record.committed_at,
                        files_added: record.file_paths.len(),
                        files_removed: 0,
                        was_idempotent: true,
                        retries: 0,
                    });
                }
            }
        }

        // Perform commit with retry
        let result = self
            .commit_with_retry(files.clone(), kafka_offsets.clone())
            .await;

        match result {
            Ok(commit_result) => {
                self.stats.commits_succeeded.fetch_add(1, Ordering::Relaxed);

                // Record idempotency
                if self.config.enable_idempotency {
                    if let Some((ref topic, partition, offset_min, offset_max)) = kafka_offsets {
                        self.record_idempotency(
                            IdempotencyKey::new(topic, partition, offset_min, offset_max),
                            commit_result.snapshot_id,
                            commit_result.committed_at,
                            files.iter().map(|f| f.file_path.clone()).collect(),
                        );
                    }
                }

                // Invalidate cache
                self.cache.invalidate_snapshot();
                self.cache.invalidate_manifest();

                // Log to transaction log
                if let Some(ref txlog) = self.txlog {
                    if let Some((ref topic, partition, offset_min, offset_max)) = kafka_offsets {
                        let entry = TransactionEntry::IdempotencyRecord {
                            kafka_offset_min: offset_min,
                            kafka_offset_max: offset_max,
                            topic: topic.clone(),
                            partition,
                            snapshot_id: commit_result.snapshot_id,
                            file_paths: files.iter().map(|f| f.file_path.clone()).collect(),
                            committed_at: commit_result.committed_at,
                        };
                        if let Err(e) = txlog.append(entry) {
                            warn!(error = %e, "Failed to write idempotency record to transaction log");
                        }
                    }
                }

                Ok(commit_result)
            }
            Err(e) => {
                self.stats.commits_failed.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Commit with retry on CAS conflict.
    async fn commit_with_retry(
        &self,
        files: Vec<DataFileInfo>,
        kafka_offsets: Option<(String, i32, i64, i64)>,
    ) -> Result<CommitResult> {
        let mut retries = 0;
        let mut last_error = None;

        while retries <= self.config.max_retries {
            // Get current snapshot ID for CAS
            let expected_snapshot_id = self
                .catalog
                .current_snapshot_id(&self.namespace, &self.table_name)
                .await?;

            // Prepare commit
            let commit = SnapshotCommit {
                expected_snapshot_id,
                files_to_add: files.clone(),
                files_to_remove: vec![],
                summary: self.build_summary(&kafka_offsets),
            };

            // Attempt commit
            match self
                .catalog
                .commit_snapshot(&self.namespace, &self.table_name, commit)
                .await
            {
                Ok(result) => {
                    if retries > 0 {
                        info!(
                            snapshot_id = result.snapshot_id,
                            retries = retries,
                            "Commit succeeded after retries"
                        );
                    }

                    return Ok(CommitResult {
                        snapshot_id: result.snapshot_id,
                        committed_at: result.committed_at,
                        files_added: result.files_added,
                        files_removed: result.files_removed,
                        was_idempotent: false,
                        retries,
                    });
                }
                Err(Error::Iceberg(IcebergError::CasConflict { expected, actual })) => {
                    self.stats.cas_conflicts.fetch_add(1, Ordering::Relaxed);
                    self.stats.retries.fetch_add(1, Ordering::Relaxed);

                    retries += 1;
                    if retries > self.config.max_retries {
                        warn!(
                            expected = expected,
                            actual = actual,
                            retries = retries,
                            "CAS conflict: max retries exceeded"
                        );
                        return Err(Error::Iceberg(IcebergError::CasConflict {
                            expected,
                            actual,
                        }));
                    }

                    // Exponential backoff
                    let delay = self.calculate_backoff(retries);
                    debug!(
                        expected = expected,
                        actual = actual,
                        retry = retries,
                        delay_ms = delay.as_millis(),
                        "CAS conflict, retrying"
                    );

                    tokio::time::sleep(delay).await;
                    last_error = Some(Error::Iceberg(IcebergError::CasConflict {
                        expected,
                        actual,
                    }));
                }
                Err(e) => {
                    // Non-retryable error
                    return Err(e);
                }
            }
        }

        // Should not reach here, but handle gracefully
        Err(last_error.unwrap_or_else(|| {
            Error::Iceberg(IcebergError::Other("Unknown commit error".to_string()))
        }))
    }

    /// Calculate backoff delay for retry.
    fn calculate_backoff(&self, retry: u32) -> Duration {
        let delay_ms = self.config.base_delay_ms * (2_u64.pow(retry.saturating_sub(1)));
        let delay_ms = delay_ms.min(self.config.max_delay_ms);
        Duration::from_millis(delay_ms)
    }

    /// Build summary properties for the snapshot.
    fn build_summary(
        &self,
        kafka_offsets: &Option<(String, i32, i64, i64)>,
    ) -> HashMap<String, String> {
        let mut summary = HashMap::new();

        summary.insert("operation".to_string(), "append".to_string());
        summary.insert("commit-timestamp".to_string(), Utc::now().to_rfc3339());

        if let Some((ref topic, partition, offset_min, offset_max)) = kafka_offsets {
            summary.insert("kafka-topic".to_string(), topic.clone());
            summary.insert("kafka-partition".to_string(), partition.to_string());
            summary.insert("kafka-offset-min".to_string(), offset_min.to_string());
            summary.insert("kafka-offset-max".to_string(), offset_max.to_string());
        }

        summary
    }

    /// Check if an offset range has already been committed.
    fn check_idempotency(&self, key: &IdempotencyKey) -> Option<IdempotencyRecord> {
        let records = self.idempotency_records.read();
        records.get(key).cloned()
    }

    /// Record an idempotency entry.
    fn record_idempotency(
        &self,
        key: IdempotencyKey,
        snapshot_id: i64,
        committed_at: DateTime<Utc>,
        file_paths: Vec<String>,
    ) {
        let mut records = self.idempotency_records.write();

        // Evict old records if at capacity
        if records.len() >= self.config.max_idempotency_records {
            // Simple eviction: remove oldest (by committed_at)
            if let Some(oldest_key) = records
                .iter()
                .min_by_key(|(_, r)| r.committed_at)
                .map(|(k, _)| k.clone())
            {
                records.remove(&oldest_key);
            }
        }

        records.insert(
            key.clone(),
            IdempotencyRecord {
                key,
                snapshot_id,
                committed_at,
                file_paths,
            },
        );
    }

    /// Check if an offset range is covered by any existing idempotency record.
    pub fn is_offset_range_committed(
        &self,
        topic: &str,
        partition: i32,
        offset_min: i64,
        offset_max: i64,
    ) -> Option<i64> {
        let records = self.idempotency_records.read();

        for record in records.values() {
            if record.key.topic == topic
                && record.key.partition == partition
                && record.key.offset_min <= offset_min
                && record.key.offset_max >= offset_max
            {
                return Some(record.snapshot_id);
            }
        }

        None
    }

    /// Load idempotency records from transaction log.
    pub async fn load_idempotency_records(&self) -> Result<usize> {
        let Some(ref txlog) = self.txlog else {
            return Ok(0);
        };

        let entries = txlog.read_all_entries()?;
        let mut count = 0;

        for entry in entries {
            if let TransactionEntry::IdempotencyRecord {
                kafka_offset_min,
                kafka_offset_max,
                topic,
                partition,
                snapshot_id,
                file_paths,
                committed_at,
            } = entry
            {
                let key =
                    IdempotencyKey::new(&topic, partition, kafka_offset_min, kafka_offset_max);
                self.record_idempotency(key, snapshot_id, committed_at, file_paths);
                count += 1;
            }
        }

        info!(
            count = count,
            "Loaded idempotency records from transaction log"
        );
        Ok(count)
    }

    /// Clear all idempotency records.
    pub fn clear_idempotency_records(&self) {
        let mut records = self.idempotency_records.write();
        records.clear();
    }

    /// Get statistics.
    pub fn stats(&self) -> &TransactionStats {
        &self.stats
    }

    /// Get the number of idempotency records.
    pub fn idempotency_record_count(&self) -> usize {
        self.idempotency_records.read().len()
    }

    /// Get the full table name.
    pub fn full_table_name(&self) -> String {
        format!("{}.{}", self.namespace, self.table_name)
    }
}

/// Result of a commit operation.
#[derive(Debug, Clone)]
pub struct CommitResult {
    /// The new snapshot ID
    pub snapshot_id: i64,
    /// When the commit was made
    pub committed_at: DateTime<Utc>,
    /// Number of files added
    pub files_added: usize,
    /// Number of files removed
    pub files_removed: usize,
    /// Whether this was an idempotent return (duplicate request)
    pub was_idempotent: bool,
    /// Number of retries before success
    pub retries: u32,
}

/// Builder for TransactionCoordinator.
pub struct TransactionCoordinatorBuilder {
    catalog: Option<Arc<dyn CatalogOperations>>,
    namespace: Option<String>,
    table_name: Option<String>,
    config: TransactionCoordinatorConfig,
    cache: Option<Arc<MetadataCache>>,
    txlog: Option<Arc<TransactionLog>>,
}

impl TransactionCoordinatorBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            catalog: None,
            namespace: None,
            table_name: None,
            config: TransactionCoordinatorConfig::default(),
            cache: None,
            txlog: None,
        }
    }

    /// Set the catalog.
    pub fn catalog(mut self, catalog: Arc<dyn CatalogOperations>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Set the namespace.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Set the table name.
    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = Some(table_name.into());
        self
    }

    /// Set the configuration.
    pub fn config(mut self, config: TransactionCoordinatorConfig) -> Self {
        self.config = config;
        self
    }

    /// Set max retries.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.config.max_retries = max_retries;
        self
    }

    /// Enable or disable idempotency.
    pub fn enable_idempotency(mut self, enable: bool) -> Self {
        self.config.enable_idempotency = enable;
        self
    }

    /// Set the cache.
    pub fn cache(mut self, cache: Arc<MetadataCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Set the transaction log.
    pub fn txlog(mut self, txlog: Arc<TransactionLog>) -> Self {
        self.txlog = Some(txlog);
        self
    }

    /// Build the TransactionCoordinator.
    pub fn build(self) -> Result<TransactionCoordinator> {
        let catalog = self.catalog.ok_or_else(|| {
            Error::Config("TransactionCoordinator requires a catalog".to_string())
        })?;
        let namespace = self.namespace.ok_or_else(|| {
            Error::Config("TransactionCoordinator requires a namespace".to_string())
        })?;
        let table_name = self.table_name.ok_or_else(|| {
            Error::Config("TransactionCoordinator requires a table name".to_string())
        })?;
        let cache = self.cache.unwrap_or_else(|| Arc::new(MetadataCache::new()));

        let mut coordinator =
            TransactionCoordinator::new(catalog, namespace, table_name, self.config, cache);

        if let Some(txlog) = self.txlog {
            coordinator = coordinator.with_txlog(txlog);
        }

        Ok(coordinator)
    }
}

impl Default for TransactionCoordinatorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CatalogType;
    use crate::iceberg::factory::{CatalogHealth, SnapshotCommitResult, TableInfo, TableSchema};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicI64, AtomicU32};

    /// Mock catalog that simulates CAS behavior.
    struct MockCatalogWithCas {
        current_snapshot: AtomicI64,
        conflict_count: AtomicU32,
        max_conflicts: u32,
    }

    impl MockCatalogWithCas {
        fn new() -> Self {
            Self {
                current_snapshot: AtomicI64::new(100),
                conflict_count: AtomicU32::new(0),
                max_conflicts: 0,
            }
        }

        fn with_conflicts(max_conflicts: u32) -> Self {
            Self {
                current_snapshot: AtomicI64::new(100),
                conflict_count: AtomicU32::new(0),
                max_conflicts,
            }
        }
    }

    #[async_trait]
    impl CatalogOperations for MockCatalogWithCas {
        async fn health_check(&self) -> Result<CatalogHealth> {
            Ok(CatalogHealth {
                is_healthy: true,
                response_time_ms: 10,
                message: None,
                catalog_type: CatalogType::Rest,
            })
        }

        async fn list_namespaces(&self) -> Result<Vec<String>> {
            Ok(vec!["test_db".to_string()])
        }

        async fn namespace_exists(&self, _namespace: &str) -> Result<bool> {
            Ok(true)
        }

        async fn create_namespace(&self, _namespace: &str) -> Result<()> {
            Ok(())
        }

        async fn list_tables(&self, _namespace: &str) -> Result<Vec<String>> {
            Ok(vec!["test_table".to_string()])
        }

        async fn table_exists(&self, _namespace: &str, _table: &str) -> Result<bool> {
            Ok(true)
        }

        async fn load_table(&self, namespace: &str, table: &str) -> Result<TableInfo> {
            Ok(TableInfo {
                namespace: namespace.to_string(),
                name: table.to_string(),
                location: format!("s3://warehouse/{}/{}", namespace, table),
                current_snapshot_id: Some(self.current_snapshot.load(Ordering::Relaxed)),
                schema: TableSchema {
                    schema_id: 1,
                    fields: vec![],
                },
                properties: HashMap::new(),
            })
        }

        async fn create_table(
            &self,
            namespace: &str,
            table: &str,
            schema: &TableSchema,
        ) -> Result<TableInfo> {
            Ok(TableInfo {
                namespace: namespace.to_string(),
                name: table.to_string(),
                location: format!("s3://warehouse/{}/{}", namespace, table),
                current_snapshot_id: None,
                schema: schema.clone(),
                properties: HashMap::new(),
            })
        }

        async fn current_snapshot_id(&self, _namespace: &str, _table: &str) -> Result<Option<i64>> {
            Ok(Some(self.current_snapshot.load(Ordering::Relaxed)))
        }

        async fn commit_snapshot(
            &self,
            _namespace: &str,
            _table: &str,
            commit: SnapshotCommit,
        ) -> Result<SnapshotCommitResult> {
            let current = self.current_snapshot.load(Ordering::Relaxed);

            // Simulate CAS conflict
            let conflicts = self.conflict_count.fetch_add(1, Ordering::Relaxed);
            if conflicts < self.max_conflicts {
                // Simulate another writer changing the snapshot
                self.current_snapshot.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Iceberg(IcebergError::CasConflict {
                    expected: commit.expected_snapshot_id.unwrap_or(-1),
                    actual: current + 1,
                }));
            }

            // Success
            let new_snapshot = self.current_snapshot.fetch_add(1, Ordering::Relaxed) + 1;
            Ok(SnapshotCommitResult {
                snapshot_id: new_snapshot,
                committed_at: Utc::now(),
                files_added: commit.files_to_add.len(),
                files_removed: commit.files_to_remove.len(),
            })
        }

        fn catalog_type(&self) -> CatalogType {
            CatalogType::Rest
        }

        fn warehouse_path(&self) -> &str {
            "s3://warehouse"
        }

        async fn close(&self) -> Result<()> {
            Ok(())
        }
    }

    fn create_test_file() -> DataFileInfo {
        DataFileInfo {
            file_path: "s3://warehouse/data/file.parquet".to_string(),
            file_size_bytes: 1024 * 1024,
            record_count: 1000,
            partition_values: HashMap::new(),
            file_format: "parquet".to_string(),
        }
    }

    #[tokio::test]
    async fn test_coordinator_creation() {
        let catalog = Arc::new(MockCatalogWithCas::new());
        let cache = Arc::new(MetadataCache::new());

        let coordinator = TransactionCoordinator::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            TransactionCoordinatorConfig::default(),
            cache,
        );

        assert_eq!(coordinator.full_table_name(), "test_db.test_table");
    }

    #[tokio::test]
    async fn test_commit_success() {
        let catalog = Arc::new(MockCatalogWithCas::new());
        let cache = Arc::new(MetadataCache::new());

        let coordinator = TransactionCoordinator::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            TransactionCoordinatorConfig::default(),
            cache,
        );

        let files = vec![create_test_file()];
        let result = coordinator.commit_snapshot(files, None).await;

        assert!(result.is_ok());
        let commit_result = result.unwrap();
        assert!(!commit_result.was_idempotent);
        assert_eq!(commit_result.retries, 0);
        assert_eq!(commit_result.files_added, 1);
    }

    #[tokio::test]
    async fn test_commit_with_retry() {
        let catalog = Arc::new(MockCatalogWithCas::with_conflicts(2)); // 2 conflicts before success
        let cache = Arc::new(MetadataCache::new());

        let config = TransactionCoordinatorConfig {
            max_retries: 3,
            base_delay_ms: 10, // Fast for testing
            max_delay_ms: 100,
            ..Default::default()
        };

        let coordinator = TransactionCoordinator::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            config,
            cache,
        );

        let files = vec![create_test_file()];
        let result = coordinator.commit_snapshot(files, None).await;

        assert!(result.is_ok());
        let commit_result = result.unwrap();
        assert_eq!(commit_result.retries, 2); // Should have retried twice
        assert_eq!(coordinator.stats().cas_conflicts.load(Ordering::Relaxed), 2);
        assert_eq!(coordinator.stats().retries.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_commit_max_retries_exceeded() {
        let catalog = Arc::new(MockCatalogWithCas::with_conflicts(10)); // More than max retries
        let cache = Arc::new(MetadataCache::new());

        let config = TransactionCoordinatorConfig {
            max_retries: 2,
            base_delay_ms: 10,
            max_delay_ms: 100,
            ..Default::default()
        };

        let coordinator = TransactionCoordinator::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            config,
            cache,
        );

        let files = vec![create_test_file()];
        let result = coordinator.commit_snapshot(files, None).await;

        assert!(result.is_err());
        assert_eq!(
            coordinator.stats().commits_failed.load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn test_idempotency() {
        let catalog = Arc::new(MockCatalogWithCas::new());
        let cache = Arc::new(MetadataCache::new());

        let config = TransactionCoordinatorConfig {
            enable_idempotency: true,
            ..Default::default()
        };

        let coordinator = TransactionCoordinator::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            config,
            cache,
        );

        let files = vec![create_test_file()];
        let offsets = Some(("topic".to_string(), 0, 100, 200));

        // First commit
        let result1 = coordinator
            .commit_snapshot(files.clone(), offsets.clone())
            .await;
        assert!(result1.is_ok());
        let commit1 = result1.unwrap();
        assert!(!commit1.was_idempotent);

        // Same offsets - should be idempotent
        let result2 = coordinator
            .commit_snapshot(files.clone(), offsets.clone())
            .await;
        assert!(result2.is_ok());
        let commit2 = result2.unwrap();
        assert!(commit2.was_idempotent);
        assert_eq!(commit2.snapshot_id, commit1.snapshot_id);

        // Check stats
        assert_eq!(
            coordinator
                .stats()
                .idempotent_returns
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn test_idempotency_disabled() {
        let catalog = Arc::new(MockCatalogWithCas::new());
        let cache = Arc::new(MetadataCache::new());

        let config = TransactionCoordinatorConfig {
            enable_idempotency: false,
            ..Default::default()
        };

        let coordinator = TransactionCoordinator::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            config,
            cache,
        );

        let files = vec![create_test_file()];
        let offsets = Some(("topic".to_string(), 0, 100, 200));

        // First commit
        let result1 = coordinator
            .commit_snapshot(files.clone(), offsets.clone())
            .await;
        assert!(result1.is_ok());
        assert!(!result1.unwrap().was_idempotent);

        // Second commit - should NOT be idempotent (feature disabled)
        let result2 = coordinator
            .commit_snapshot(files.clone(), offsets.clone())
            .await;
        assert!(result2.is_ok());
        assert!(!result2.unwrap().was_idempotent);
    }

    #[tokio::test]
    async fn test_is_offset_range_committed() {
        let catalog = Arc::new(MockCatalogWithCas::new());
        let cache = Arc::new(MetadataCache::new());

        let coordinator = TransactionCoordinator::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            TransactionCoordinatorConfig::default(),
            cache,
        );

        let files = vec![create_test_file()];
        let offsets = Some(("topic".to_string(), 0, 100, 200));

        // Commit
        let _result = coordinator.commit_snapshot(files, offsets).await.unwrap();

        // Check various ranges
        assert!(coordinator
            .is_offset_range_committed("topic", 0, 100, 200)
            .is_some());
        assert!(coordinator
            .is_offset_range_committed("topic", 0, 150, 180)
            .is_some()); // Subset
        assert!(coordinator
            .is_offset_range_committed("topic", 0, 50, 150)
            .is_none()); // Not covered
        assert!(coordinator
            .is_offset_range_committed("other_topic", 0, 100, 200)
            .is_none());
    }

    #[tokio::test]
    async fn test_idempotency_record_eviction() {
        let catalog = Arc::new(MockCatalogWithCas::new());
        let cache = Arc::new(MetadataCache::new());

        let config = TransactionCoordinatorConfig {
            max_idempotency_records: 3,
            ..Default::default()
        };

        let coordinator = TransactionCoordinator::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            config,
            cache,
        );

        // Commit 5 different offset ranges
        for i in 0..5 {
            let files = vec![create_test_file()];
            let offsets = Some(("topic".to_string(), 0, i * 100, (i + 1) * 100 - 1));
            let _ = coordinator.commit_snapshot(files, offsets).await;
        }

        // Should only have 3 records (max)
        assert_eq!(coordinator.idempotency_record_count(), 3);
    }

    #[tokio::test]
    async fn test_clear_idempotency_records() {
        let catalog = Arc::new(MockCatalogWithCas::new());
        let cache = Arc::new(MetadataCache::new());

        let coordinator = TransactionCoordinator::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            TransactionCoordinatorConfig::default(),
            cache,
        );

        let files = vec![create_test_file()];
        let offsets = Some(("topic".to_string(), 0, 100, 200));
        let _ = coordinator.commit_snapshot(files, offsets).await;

        assert_eq!(coordinator.idempotency_record_count(), 1);

        coordinator.clear_idempotency_records();

        assert_eq!(coordinator.idempotency_record_count(), 0);
    }

    #[test]
    fn test_calculate_backoff() {
        let catalog = Arc::new(MockCatalogWithCas::new());
        let cache = Arc::new(MetadataCache::new());

        let config = TransactionCoordinatorConfig {
            base_delay_ms: 100,
            max_delay_ms: 5000,
            ..Default::default()
        };

        let coordinator = TransactionCoordinator::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            config,
            cache,
        );

        // Retry 1: 100ms
        assert_eq!(coordinator.calculate_backoff(1), Duration::from_millis(100));
        // Retry 2: 200ms
        assert_eq!(coordinator.calculate_backoff(2), Duration::from_millis(200));
        // Retry 3: 400ms
        assert_eq!(coordinator.calculate_backoff(3), Duration::from_millis(400));
        // Retry 10: capped at 5000ms
        assert_eq!(
            coordinator.calculate_backoff(10),
            Duration::from_millis(5000)
        );
    }

    #[tokio::test]
    async fn test_builder_pattern() {
        let catalog = Arc::new(MockCatalogWithCas::new());
        let cache = Arc::new(MetadataCache::new());

        let coordinator = TransactionCoordinatorBuilder::new()
            .catalog(catalog)
            .namespace("my_db")
            .table_name("my_table")
            .max_retries(5)
            .enable_idempotency(true)
            .cache(cache)
            .build()
            .unwrap();

        assert_eq!(coordinator.full_table_name(), "my_db.my_table");
    }

    #[tokio::test]
    async fn test_stats() {
        let catalog = Arc::new(MockCatalogWithCas::new());
        let cache = Arc::new(MetadataCache::new());

        let coordinator = TransactionCoordinator::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            TransactionCoordinatorConfig::default(),
            cache,
        );

        // Initial stats
        assert_eq!(
            coordinator
                .stats()
                .commits_attempted
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(coordinator.stats().success_rate(), 1.0);

        // After one successful commit
        let files = vec![create_test_file()];
        let _ = coordinator.commit_snapshot(files, None).await;

        assert_eq!(
            coordinator
                .stats()
                .commits_attempted
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            coordinator
                .stats()
                .commits_succeeded
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(coordinator.stats().success_rate(), 1.0);
    }
}
