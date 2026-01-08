//! Iceberg writer with atomic commits.
//!
//! Handles Arrow → Parquet conversion, S3 upload, and atomic Iceberg commits.
//!
//! ## Write Flow
//!
//! 1. Convert Arrow RecordBatch to Parquet bytes (Snappy compression)
//! 2. Upload Parquet file to object storage (S3/GCS/Azure)
//! 3. Commit file to Iceberg catalog atomically
//! 4. Return snapshot ID for transaction log
//!
//! ## Catalog Integration
//!
//! When configured with catalog components, the writer provides:
//! - Real Iceberg catalog operations via CatalogOperations trait
//! - Table lifecycle management via TableManager
//! - Atomic commits with CAS semantics via TransactionCoordinator
//! - Metadata caching via MetadataCache

use crate::config::{IcebergConfig, ParquetCompression};
use crate::iceberg::factory::{CatalogOperations, DataFileInfo, SnapshotCommit};
use crate::iceberg::metadata_cache::SharedMetadataCache;
use crate::iceberg::table_manager::TableManager;
use crate::iceberg::transaction_coordinator::TransactionCoordinator;
use crate::txlog::{TransactionEntry, TransactionLog};
use crate::{Error, IcebergError, Result};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use crc32fast::Hasher;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};
use uuid::Uuid;

/// Statistics from a write operation.
#[derive(Debug, Clone)]
pub struct WriteStats {
    /// Number of rows written
    pub row_count: usize,
    /// Parquet file size in bytes
    pub file_size_bytes: usize,
    /// Time spent converting to Parquet
    pub parquet_conversion_duration: std::time::Duration,
    /// Time spent uploading to storage
    pub upload_duration: std::time::Duration,
    /// Time spent committing to catalog
    pub commit_duration: std::time::Duration,
    /// Total write duration
    pub total_duration: std::time::Duration,
    /// File path in object storage
    pub file_path: String,
    /// Snapshot ID
    pub snapshot_id: i64,
}

/// Partition information for dual partitioning (Kafka + Iceberg).
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Kafka partition number
    pub kafka_partition: i32,
    /// Event timestamp (Unix epoch milliseconds)
    pub event_timestamp_ms: i64,
    /// Min offset in this batch
    pub min_offset: i64,
    /// Max offset in this batch
    pub max_offset: i64,
}

/// Iceberg writer with atomic commit support.
pub struct IcebergWriter {
    config: IcebergConfig,
    object_store: Arc<dyn ObjectStore>,
    txlog: Option<Arc<TransactionLog>>,
    write_count: AtomicU64,
    /// Optional catalog operations for real catalog integration
    catalog: Option<Arc<dyn CatalogOperations>>,
    /// Optional table manager for table lifecycle
    #[allow(dead_code)]
    table_manager: Option<Arc<TableManager>>,
    /// Optional transaction coordinator for atomic commits
    transaction_coordinator: Option<Arc<TransactionCoordinator>>,
    /// Optional metadata cache
    metadata_cache: Option<SharedMetadataCache>,
}

/// Builder for IcebergWriter.
pub struct IcebergWriterBuilder {
    config: IcebergConfig,
    txlog: Option<Arc<TransactionLog>>,
    catalog: Option<Arc<dyn CatalogOperations>>,
    table_manager: Option<Arc<TableManager>>,
    transaction_coordinator: Option<Arc<TransactionCoordinator>>,
    metadata_cache: Option<SharedMetadataCache>,
}

impl IcebergWriterBuilder {
    /// Create a new builder with the given configuration.
    pub fn new(config: IcebergConfig) -> Self {
        Self {
            config,
            txlog: None,
            catalog: None,
            table_manager: None,
            transaction_coordinator: None,
            metadata_cache: None,
        }
    }

    /// Set the transaction log.
    pub fn with_txlog(mut self, txlog: Arc<TransactionLog>) -> Self {
        self.txlog = Some(txlog);
        self
    }

    /// Set the catalog operations.
    pub fn with_catalog(mut self, catalog: Arc<dyn CatalogOperations>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Set the table manager.
    pub fn with_table_manager(mut self, table_manager: Arc<TableManager>) -> Self {
        self.table_manager = Some(table_manager);
        self
    }

    /// Set the transaction coordinator.
    pub fn with_transaction_coordinator(
        mut self,
        coordinator: Arc<TransactionCoordinator>,
    ) -> Self {
        self.transaction_coordinator = Some(coordinator);
        self
    }

    /// Set the metadata cache.
    pub fn with_metadata_cache(mut self, cache: SharedMetadataCache) -> Self {
        self.metadata_cache = Some(cache);
        self
    }

    /// Build the IcebergWriter.
    pub async fn build(self) -> Result<IcebergWriter> {
        let object_store = IcebergWriter::create_object_store(&self.config)?;

        Ok(IcebergWriter {
            config: self.config,
            object_store,
            txlog: self.txlog,
            write_count: AtomicU64::new(0),
            catalog: self.catalog,
            table_manager: self.table_manager,
            transaction_coordinator: self.transaction_coordinator,
            metadata_cache: self.metadata_cache,
        })
    }
}

impl IcebergWriter {
    /// Create a new Iceberg writer.
    pub async fn new(config: IcebergConfig) -> Result<Self> {
        IcebergWriterBuilder::new(config).build().await
    }

    /// Create a new Iceberg writer with transaction log integration.
    pub async fn with_txlog(config: IcebergConfig, txlog: Arc<TransactionLog>) -> Result<Self> {
        IcebergWriterBuilder::new(config)
            .with_txlog(txlog)
            .build()
            .await
    }

    /// Create a builder for more advanced configuration.
    pub fn builder(config: IcebergConfig) -> IcebergWriterBuilder {
        IcebergWriterBuilder::new(config)
    }

    /// Check if catalog integration is enabled.
    pub fn has_catalog_integration(&self) -> bool {
        self.catalog.is_some()
    }

    /// Get the transaction coordinator if configured.
    pub fn transaction_coordinator(&self) -> Option<&Arc<TransactionCoordinator>> {
        self.transaction_coordinator.as_ref()
    }

    /// Get the metadata cache if configured.
    pub fn metadata_cache(&self) -> Option<&SharedMetadataCache> {
        self.metadata_cache.as_ref()
    }

    /// Create object store based on configuration.
    fn create_object_store(config: &IcebergConfig) -> Result<Arc<dyn ObjectStore>> {
        let warehouse_path = &config.warehouse_path;

        if warehouse_path.starts_with("s3://") {
            Self::create_s3_store(config)
        } else if warehouse_path.starts_with("gs://") {
            Self::create_gcs_store(config)
        } else if warehouse_path.starts_with("az://") || warehouse_path.starts_with("abfs://") {
            Self::create_azure_store(config)
        } else {
            // Local filesystem for development/testing
            Self::create_local_store(config)
        }
    }

    fn create_s3_store(config: &IcebergConfig) -> Result<Arc<dyn ObjectStore>> {
        use object_store::aws::AmazonS3Builder;

        let bucket = config
            .warehouse_path
            .strip_prefix("s3://")
            .and_then(|s| s.split('/').next())
            .ok_or_else(|| {
                Error::Iceberg(IcebergError::CatalogConnection("Invalid S3 path".into()))
            })?;

        let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);

        if let Some(ref region) = config.aws_region {
            builder = builder.with_region(region);
        }

        if let Some(ref access_key) = config.aws_access_key_id {
            builder = builder.with_access_key_id(access_key);
        }

        if let Some(ref secret_key) = config.aws_secret_access_key {
            builder = builder.with_secret_access_key(secret_key);
        }

        if let Some(ref endpoint) = config.s3_endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_allow_http(endpoint.starts_with("http://"));
        }

        let store = builder
            .build()
            .map_err(|e| Error::Iceberg(IcebergError::FileUpload(e.to_string())))?;

        Ok(Arc::new(store))
    }

    fn create_gcs_store(_config: &IcebergConfig) -> Result<Arc<dyn ObjectStore>> {
        // GCS support - for now just return an error, implement when needed
        Err(Error::Iceberg(IcebergError::CatalogConnection(
            "GCS storage not yet implemented".into(),
        )))
    }

    fn create_azure_store(_config: &IcebergConfig) -> Result<Arc<dyn ObjectStore>> {
        // Azure support - for now just return an error, implement when needed
        Err(Error::Iceberg(IcebergError::CatalogConnection(
            "Azure storage not yet implemented".into(),
        )))
    }

    fn create_local_store(config: &IcebergConfig) -> Result<Arc<dyn ObjectStore>> {
        use object_store::local::LocalFileSystem;

        let path = std::path::Path::new(&config.warehouse_path);

        // Create directory if it doesn't exist
        if !path.exists() {
            std::fs::create_dir_all(path).map_err(|e| {
                Error::Iceberg(IcebergError::FileUpload(format!(
                    "Failed to create local warehouse directory: {}",
                    e
                )))
            })?;
        }

        let store = LocalFileSystem::new_with_prefix(path).map_err(|e| {
            Error::Iceberg(IcebergError::FileUpload(format!(
                "Failed to create local file system store: {}",
                e
            )))
        })?;

        Ok(Arc::new(store))
    }

    /// Write a RecordBatch to Iceberg.
    ///
    /// This performs the following steps:
    /// 1. Convert RecordBatch to Parquet with Snappy compression
    /// 2. Upload to object storage
    /// 3. Commit to Iceberg catalog (TODO: actual catalog integration)
    /// 4. Log to transaction log if configured
    ///
    /// Returns the snapshot ID for the committed data.
    pub async fn write_batch(&self, batch: RecordBatch, kafka_offset: i64) -> Result<WriteStats> {
        let total_start = Instant::now();
        let row_count = batch.num_rows();

        if row_count == 0 {
            return Err(Error::Iceberg(IcebergError::ParquetWrite(
                "Cannot write empty batch".into(),
            )));
        }

        // Extract partition info for dual partitioning (Kafka + Iceberg)
        let partition_info = self.extract_partition_info(&batch)?;

        // Generate unique batch ID and file path with partition info
        let batch_id = Uuid::new_v4().to_string();
        let file_path = self.generate_file_path(&partition_info);

        debug!(
            kafka_partition = partition_info.kafka_partition,
            offset_range = %format!("{}-{}", partition_info.min_offset, partition_info.max_offset),
            "Extracted partition info for dual partitioning"
        );

        // Log flush start if txlog configured
        if let Some(ref txlog) = self.txlog {
            txlog.append(TransactionEntry::FlushStart {
                batch_id: batch_id.clone(),
                row_count: row_count as u32,
                timestamp: chrono::Utc::now(),
            })?;
        }

        // Step 1: Convert to Parquet
        let parquet_start = Instant::now();
        let parquet_bytes = self.convert_to_parquet(&batch)?;
        let parquet_duration = parquet_start.elapsed();
        let file_size_bytes = parquet_bytes.len();

        // Calculate CRC32 checksum
        let mut hasher = Hasher::new();
        hasher.update(&parquet_bytes);
        let checksum = format!("{:08x}", hasher.finalize());

        debug!(
            rows = row_count,
            size_bytes = file_size_bytes,
            duration_ms = parquet_duration.as_millis(),
            "Converted batch to Parquet"
        );

        // Step 2: Upload to object storage
        let upload_start = Instant::now();
        self.upload_file(&file_path, parquet_bytes).await?;
        let upload_duration = upload_start.elapsed();

        // Log file written if txlog configured
        if let Some(ref txlog) = self.txlog {
            txlog.append(TransactionEntry::ParquetWritten {
                batch_id: batch_id.clone(),
                file_path: file_path.clone(),
                file_size_bytes: file_size_bytes as u64,
                row_count: row_count as u32,
                checksum,
                timestamp: chrono::Utc::now(),
            })?;
        }

        debug!(
            path = %file_path,
            duration_ms = upload_duration.as_millis(),
            "Uploaded Parquet file"
        );

        // Step 3: Commit to Iceberg catalog with partition info
        let commit_start = Instant::now();
        let snapshot_id = self
            .commit_to_catalog(
                &file_path,
                file_size_bytes as u64,
                row_count as u64,
                kafka_offset,
                &partition_info,
            )
            .await?;
        let commit_duration = commit_start.elapsed();

        // Log snapshot if txlog configured
        if let Some(ref txlog) = self.txlog {
            txlog.append(TransactionEntry::IcebergSnapshot {
                batch_id: batch_id.clone(),
                snapshot_id,
                manifest_list_path: format!(
                    "{}/metadata/snap-{}.avro",
                    self.config.warehouse_path, snapshot_id
                ),
                row_count_total: row_count as u64,
                timestamp: chrono::Utc::now(),
            })?;

            // Log flush complete
            let total_duration = total_start.elapsed();
            txlog.append(TransactionEntry::FlushComplete {
                batch_id: batch_id.clone(),
                kafka_offset,
                iceberg_snapshot_id: snapshot_id,
                duration_ms: total_duration.as_millis() as u64,
                timestamp: chrono::Utc::now(),
            })?;
        }

        let total_duration = total_start.elapsed();
        self.write_count.fetch_add(1, Ordering::Relaxed);

        info!(
            rows = row_count,
            file_size_mb = file_size_bytes as f64 / 1_000_000.0,
            snapshot_id = snapshot_id,
            total_ms = total_duration.as_millis(),
            path = %file_path,
            "Batch written to Iceberg"
        );

        Ok(WriteStats {
            row_count,
            file_size_bytes,
            parquet_conversion_duration: parquet_duration,
            upload_duration,
            commit_duration,
            total_duration,
            file_path,
            snapshot_id,
        })
    }

    /// Convert an Arrow RecordBatch to Parquet bytes.
    fn convert_to_parquet(&self, batch: &RecordBatch) -> Result<Bytes> {
        let mut buffer = Cursor::new(Vec::new());

        // Configure Parquet writer properties
        let compression = match self.config.compression {
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Gzip => Compression::GZIP(Default::default()),
            ParquetCompression::Lz4 => Compression::LZ4,
            ParquetCompression::Zstd => Compression::ZSTD(Default::default()),
            ParquetCompression::None => Compression::UNCOMPRESSED,
        };

        let props = WriterProperties::builder()
            .set_compression(compression)
            .set_max_row_group_size(128 * 1024) // 128K rows per row group
            .set_write_batch_size(1024)
            .build();

        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).map_err(|e| {
                Error::Iceberg(IcebergError::ParquetWrite(format!(
                    "Failed to create Parquet writer: {}",
                    e
                )))
            })?;

        writer.write(batch).map_err(|e| {
            Error::Iceberg(IcebergError::ParquetWrite(format!(
                "Failed to write batch to Parquet: {}",
                e
            )))
        })?;

        writer.close().map_err(|e| {
            Error::Iceberg(IcebergError::ParquetWrite(format!(
                "Failed to close Parquet writer: {}",
                e
            )))
        })?;

        Ok(Bytes::from(buffer.into_inner()))
    }

    /// Upload a file to object storage.
    async fn upload_file(&self, path: &str, data: Bytes) -> Result<()> {
        let object_path = ObjectPath::from(path);
        let payload = PutPayload::from_bytes(data);

        self.object_store
            .put(&object_path, payload)
            .await
            .map_err(|e| {
                Error::Iceberg(IcebergError::FileUpload(format!(
                    "Failed to upload file to {}: {}",
                    path, e
                )))
            })?;

        Ok(())
    }

    /// Commit file to Iceberg catalog.
    ///
    /// When catalog integration is configured, this performs real atomic commits.
    /// Otherwise, it falls back to generating a mock snapshot ID.
    async fn commit_to_catalog(
        &self,
        file_path: &str,
        file_size_bytes: u64,
        row_count: u64,
        kafka_offset: i64,
        partition_info: &PartitionInfo,
    ) -> Result<i64> {
        let namespace = &self.config.database_name;
        let table = &self.config.table_name;

        // If we have catalog integration, use real catalog operations
        if let Some(ref catalog) = self.catalog {
            return self
                .commit_with_catalog(
                    catalog,
                    file_path,
                    file_size_bytes,
                    row_count,
                    kafka_offset,
                    partition_info,
                )
                .await;
        }

        // Fall back to mock commit
        debug!(
            file_path = file_path,
            kafka_offset = kafka_offset,
            table = %format!("{}.{}", namespace, table),
            "Committing to Iceberg catalog (mock - no catalog configured)"
        );

        let snapshot_id = chrono::Utc::now().timestamp_millis();
        Ok(snapshot_id)
    }

    /// Commit with real catalog operations.
    async fn commit_with_catalog(
        &self,
        catalog: &Arc<dyn CatalogOperations>,
        file_path: &str,
        file_size_bytes: u64,
        row_count: u64,
        kafka_offset: i64,
        partition_info: &PartitionInfo,
    ) -> Result<i64> {
        let namespace = &self.config.database_name;
        let table = &self.config.table_name;

        // Get current snapshot ID for CAS
        let current_snapshot_id = if let Some(ref cache) = self.metadata_cache {
            // Try cache first
            cache.get_snapshot_id().or({
                // Cache miss - we'll fetch from catalog
                None
            })
        } else {
            None
        };

        // If cache miss, fetch from catalog
        let expected_snapshot_id = match current_snapshot_id {
            Some(id) => Some(id),
            None => catalog.current_snapshot_id(namespace, table).await?,
        };

        // Build partition values for dual partitioning (Kafka + Iceberg)
        use crate::config::PartitionStrategy;
        use chrono::{DateTime, Utc};

        let event_time = DateTime::from_timestamp_millis(partition_info.event_timestamp_ms)
            .unwrap_or_else(Utc::now);

        let mut partition_values = HashMap::new();

        // Add time-based partition value
        match self.config.table_management.partition_strategy {
            PartitionStrategy::Hourly => {
                partition_values.insert(
                    "event_hour".to_string(),
                    event_time.format("%Y-%m-%d-%H").to_string(),
                );
            }
            PartitionStrategy::Daily | PartitionStrategy::Identity | PartitionStrategy::Bucket => {
                partition_values.insert(
                    "event_date".to_string(),
                    event_time.format("%Y-%m-%d").to_string(),
                );
            }
        }

        // Add Kafka partition value
        partition_values.insert(
            "kafka_partition".to_string(),
            partition_info.kafka_partition.to_string(),
        );

        // Build the commit
        let data_file = DataFileInfo {
            file_path: file_path.to_string(),
            file_size_bytes,
            record_count: row_count,
            partition_values,
            file_format: "parquet".to_string(),
        };

        let mut summary = HashMap::new();
        summary.insert("operation".to_string(), "append".to_string());
        summary.insert("added-data-files".to_string(), "1".to_string());
        summary.insert("added-records".to_string(), row_count.to_string());
        summary.insert("kafka-offset".to_string(), kafka_offset.to_string());

        let commit = SnapshotCommit {
            expected_snapshot_id,
            files_to_add: vec![data_file],
            files_to_remove: vec![],
            summary,
        };

        // Use transaction coordinator if available for retry logic
        let result = if let Some(ref coordinator) = self.transaction_coordinator {
            // Use coordinator for idempotency and retry
            // Pass kafka offset info for idempotency tracking (with actual partition info)
            let kafka_offset_info = Some((
                self.config.table_name.clone(), // topic approximation (table name)
                partition_info.kafka_partition, // actual Kafka partition
                partition_info.min_offset,      // min offset in this batch
                partition_info.max_offset,      // max offset in this batch
            ));
            let commit_result = coordinator
                .commit_snapshot(vec![commit.files_to_add[0].clone()], kafka_offset_info)
                .await?;
            commit_result.snapshot_id
        } else {
            // Direct commit to catalog
            let commit_result = catalog.commit_snapshot(namespace, table, commit).await?;
            commit_result.snapshot_id
        };

        // Update cache with new snapshot ID
        if let Some(ref cache) = self.metadata_cache {
            cache.set_snapshot_id(result);
        }

        info!(
            namespace = %namespace,
            table = %table,
            snapshot_id = result,
            file_path = %file_path,
            kafka_offset = kafka_offset,
            "Committed to Iceberg catalog"
        );

        Ok(result)
    }

    /// Generate a unique file path for a new Parquet file with dual partitioning.
    ///
    /// Path format: data/{db}/{table}/event_date={YYYY-MM-DD}/kafka_partition={N}/part-{uuid}-{offset_range}.parquet
    ///
    /// This preserves both:
    /// - Kafka partition info (for replay capability)
    /// - Time-based partitioning (for Iceberg query optimization)
    fn generate_file_path(&self, partition_info: &PartitionInfo) -> String {
        use crate::config::PartitionStrategy;
        use chrono::{DateTime, Utc};

        let uuid = Uuid::new_v4();

        // Convert timestamp to date/hour based on partition strategy
        let event_time = DateTime::from_timestamp_millis(partition_info.event_timestamp_ms)
            .unwrap_or_else(Utc::now);

        // Generate time partition based on strategy
        let time_partition = match self.config.table_management.partition_strategy {
            PartitionStrategy::Hourly => {
                format!("event_hour={}", event_time.format("%Y-%m-%d-%H"))
            }
            PartitionStrategy::Daily | PartitionStrategy::Identity | PartitionStrategy::Bucket => {
                format!("event_date={}", event_time.format("%Y-%m-%d"))
            }
        };

        // Include offset range in filename for debugging/tracking
        let offset_range = format!(
            "{}-{}",
            partition_info.min_offset, partition_info.max_offset
        );

        // Format: data/{db}/{table}/{time_partition}/kafka_partition={N}/part-{uuid}-{offset_range}.parquet
        format!(
            "data/{}/{}/{}/kafka_partition={}/part-{}-{}.parquet",
            self.config.database_name,
            self.config.table_name,
            time_partition,
            partition_info.kafka_partition,
            uuid,
            offset_range
        )
    }

    /// Extract partition information from a RecordBatch.
    ///
    /// Extracts kafka partition, timestamp, and offset range from the batch columns.
    fn extract_partition_info(&self, batch: &RecordBatch) -> Result<PartitionInfo> {
        use arrow::array::{Int32Array, Int64Array};

        // Extract partition column
        let kafka_partition = batch
            .column_by_name("partition")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .map(|arr| if !arr.is_empty() { arr.value(0) } else { 0 })
            .unwrap_or(0);

        // Extract offset column for min/max
        let (min_offset, max_offset) = batch
            .column_by_name("offset")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .map(|arr| {
                if !arr.is_empty() {
                    let min = arr.value(0);
                    let max = arr.value(arr.len() - 1);
                    (min, max)
                } else {
                    (0, 0)
                }
            })
            .unwrap_or((0, 0));

        // Extract timestamp column (use first record's timestamp for partitioning)
        let event_timestamp_ms = batch
            .column_by_name("timestamp")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .map(|arr| {
                if !arr.is_empty() {
                    arr.value(0)
                } else {
                    chrono::Utc::now().timestamp_millis()
                }
            })
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

        Ok(PartitionInfo {
            kafka_partition,
            event_timestamp_ms,
            min_offset,
            max_offset,
        })
    }

    /// Get the write count.
    pub fn write_count(&self) -> u64 {
        self.write_count.load(Ordering::Relaxed)
    }

    /// Get the configured warehouse path.
    pub fn warehouse_path(&self) -> &str {
        &self.config.warehouse_path
    }

    /// Check if the writer is properly configured.
    pub fn is_configured(&self) -> bool {
        !self.config.warehouse_path.is_empty()
            && !self.config.database_name.is_empty()
            && !self.config.table_name.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as StdArc;
    use tempfile::TempDir;

    fn create_test_config(warehouse_path: &str) -> IcebergConfig {
        IcebergConfig {
            catalog_type: crate::config::CatalogType::Rest,
            warehouse_path: warehouse_path.to_string(),
            database_name: "test_db".to_string(),
            table_name: "test_table".to_string(),
            target_file_size_mb: 128,
            compression: ParquetCompression::Snappy,
            partition_spec: vec![],
            rest_uri: None,
            hive_metastore_uri: None,
            aws_region: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            s3_endpoint: None,
            catalog_manager: Default::default(),
            table_management: Default::default(),
            rest: Default::default(),
            glue: Default::default(),
            nessie: None,
        }
    }

    fn create_test_batch() -> RecordBatch {
        use arrow::array::Int64Array;

        // Include partition, offset, and timestamp columns for dual partitioning
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("partition", DataType::Int32, false),
            Field::new("offset", DataType::Int64, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec![
            Some("alice"),
            Some("bob"),
            Some("charlie"),
            None,
            Some("eve"),
        ]);
        // All records from partition 0
        let partition_array = Int32Array::from(vec![0, 0, 0, 0, 0]);
        // Sequential offsets
        let offset_array = Int64Array::from(vec![100, 101, 102, 103, 104]);
        // Timestamps (epoch ms)
        let now_ms = chrono::Utc::now().timestamp_millis();
        let timestamp_array = Int64Array::from(vec![now_ms, now_ms, now_ms, now_ms, now_ms]);

        RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(id_array),
                StdArc::new(name_array),
                StdArc::new(partition_array),
                StdArc::new(offset_array),
                StdArc::new(timestamp_array),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_convert_to_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(temp_dir.path().to_str().unwrap());

        let rt = tokio::runtime::Runtime::new().unwrap();
        let writer = rt.block_on(IcebergWriter::new(config)).unwrap();

        let batch = create_test_batch();
        let parquet_bytes = writer.convert_to_parquet(&batch).unwrap();

        // Verify we got valid Parquet bytes
        assert!(!parquet_bytes.is_empty());
        // Parquet files start with magic bytes "PAR1"
        assert_eq!(&parquet_bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_generate_file_path() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(temp_dir.path().to_str().unwrap());

        let rt = tokio::runtime::Runtime::new().unwrap();
        let writer = rt.block_on(IcebergWriter::new(config)).unwrap();

        // Create test partition info
        let partition_info1 = PartitionInfo {
            kafka_partition: 0,
            event_timestamp_ms: chrono::Utc::now().timestamp_millis(),
            min_offset: 100,
            max_offset: 200,
        };
        let partition_info2 = PartitionInfo {
            kafka_partition: 1,
            event_timestamp_ms: chrono::Utc::now().timestamp_millis(),
            min_offset: 300,
            max_offset: 400,
        };

        let path1 = writer.generate_file_path(&partition_info1);
        let path2 = writer.generate_file_path(&partition_info2);

        // Paths should be unique (different partitions and UUIDs)
        assert_ne!(path1, path2);

        // Paths should have correct structure with dual partitioning
        // Format: data/{db}/{table}/event_date={YYYY-MM-DD}/kafka_partition={N}/part-{uuid}-{offset_range}.parquet
        assert!(path1.starts_with("data/test_db/test_table/event_date="));
        assert!(path1.contains("/kafka_partition=0/"));
        assert!(path1.ends_with(".parquet"));

        assert!(path2.contains("/kafka_partition=1/"));

        // Verify offset range is in filename
        assert!(path1.contains("100-200"));
        assert!(path2.contains("300-400"));
    }

    #[tokio::test]
    async fn test_write_batch_local() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(temp_dir.path().to_str().unwrap());

        let writer = IcebergWriter::new(config).await.unwrap();
        let batch = create_test_batch();

        let stats = writer.write_batch(batch, 104).await.unwrap();

        assert_eq!(stats.row_count, 5);
        assert!(stats.file_size_bytes > 0);
        assert!(stats.snapshot_id > 0);
        assert!(stats.file_path.ends_with(".parquet"));

        // Verify dual partitioning path structure
        assert!(
            stats.file_path.contains("/kafka_partition=0/"),
            "Path should contain kafka_partition=0"
        );
        assert!(
            stats.file_path.contains("event_date="),
            "Path should contain event_date partition"
        );
        assert!(
            stats.file_path.contains("100-104"),
            "Path should contain offset range"
        );
    }

    #[tokio::test]
    async fn test_write_batch_empty_fails() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(temp_dir.path().to_str().unwrap());

        let writer = IcebergWriter::new(config).await.unwrap();

        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let empty_batch = RecordBatch::new_empty(schema);

        let result = writer.write_batch(empty_batch, 0).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_is_configured() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(temp_dir.path().to_str().unwrap());

        let rt = tokio::runtime::Runtime::new().unwrap();
        let writer = rt.block_on(IcebergWriter::new(config)).unwrap();

        assert!(writer.is_configured());
    }

    #[test]
    fn test_compression_types() {
        let temp_dir = TempDir::new().unwrap();
        let batch = create_test_batch();

        for compression in [
            ParquetCompression::Snappy,
            ParquetCompression::Gzip,
            ParquetCompression::Zstd,
            ParquetCompression::Lz4,
            ParquetCompression::None,
        ] {
            let mut config = create_test_config(temp_dir.path().to_str().unwrap());
            config.compression = compression.clone();

            let rt = tokio::runtime::Runtime::new().unwrap();
            let writer = rt.block_on(IcebergWriter::new(config)).unwrap();

            let result = writer.convert_to_parquet(&batch);
            assert!(result.is_ok(), "Failed for compression {:?}", compression);
        }
    }

    #[tokio::test]
    async fn test_builder_pattern() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(temp_dir.path().to_str().unwrap());

        let writer = IcebergWriter::builder(config).build().await.unwrap();

        assert!(!writer.has_catalog_integration());
        assert!(writer.transaction_coordinator().is_none());
        assert!(writer.metadata_cache().is_none());
    }

    #[tokio::test]
    async fn test_builder_with_txlog() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(temp_dir.path().to_str().unwrap());

        let txlog_path = temp_dir.path().join("txlog");
        std::fs::create_dir_all(&txlog_path).unwrap();
        let txlog_config = crate::config::TransactionLogConfig {
            log_dir: txlog_path.clone(),
            ..Default::default()
        };
        let txlog = Arc::new(crate::txlog::TransactionLog::open(txlog_config).unwrap());

        let writer = IcebergWriter::builder(config)
            .with_txlog(txlog)
            .build()
            .await
            .unwrap();

        assert!(!writer.has_catalog_integration());
    }

    #[tokio::test]
    async fn test_builder_with_metadata_cache() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(temp_dir.path().to_str().unwrap());

        let cache = crate::iceberg::metadata_cache::create_shared_cache();

        let writer = IcebergWriter::builder(config)
            .with_metadata_cache(cache)
            .build()
            .await
            .unwrap();

        assert!(writer.metadata_cache().is_some());
    }

    #[tokio::test]
    async fn test_write_batch_without_catalog() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(temp_dir.path().to_str().unwrap());

        let writer = IcebergWriter::builder(config).build().await.unwrap();
        let batch = create_test_batch();

        // Write should succeed even without catalog (mock commit)
        let stats = writer.write_batch(batch, 100).await.unwrap();

        assert_eq!(stats.row_count, 5);
        assert!(stats.snapshot_id > 0);
        assert!(!writer.has_catalog_integration());
    }
}
