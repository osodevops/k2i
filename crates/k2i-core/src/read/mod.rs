//! Real-time read-state registry.
//!
//! The registry owns the table-scoped read LSN and bridges the hot buffer to
//! committed Iceberg data files for the Unix socket RPC server.

use crate::buffer::{BufferedRecord, HotBuffer};
use crate::config::Config;
use crate::txlog::RecoveredReadDataFile;
use crate::{Error, Result};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use k2i_rpc::{DataFileRef, PartitionWatermark, ReadState, TableSummary};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// File metadata registered after an Iceberg commit becomes durable.
#[derive(Debug, Clone)]
pub struct RegisterDataFile {
    /// Writer-returned path, relative to the warehouse for local/object stores.
    pub file_path: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Number of rows in the file.
    pub row_count: u64,
    /// Kafka topic.
    pub topic: String,
    /// Kafka partition.
    pub partition: i32,
    /// Minimum Kafka offset in the file.
    pub min_offset: i64,
    /// Maximum Kafka offset in the file.
    pub max_offset: i64,
    /// Minimum table read LSN in the file.
    pub min_lsn: u64,
    /// Maximum table read LSN in the file.
    pub max_lsn: u64,
    /// Iceberg snapshot ID that committed the file.
    pub snapshot_id: i64,
}

#[derive(Debug, Clone)]
struct ActiveScan {
    file_paths: Vec<String>,
    created_at: Instant,
}

#[derive(Debug, Clone)]
struct InflightFlush {
    records: Vec<BufferedRecord>,
    batch: Option<RecordBatch>,
}

/// Registry for the single configured K2I table.
pub struct TableReadRegistry {
    database: String,
    table: String,
    warehouse_path: String,
    hot_buffer: Arc<HotBuffer>,
    current_lsn: AtomicU64,
    next_scan_id: AtomicU64,
    next_flush_id: AtomicU64,
    data_files: RwLock<Vec<DataFileRef>>,
    inflight_flushes: RwLock<HashMap<u64, InflightFlush>>,
    active_scans: RwLock<HashMap<u64, ActiveScan>>,
    max_concurrent_scans: usize,
    scan_ttl: Duration,
}

impl TableReadRegistry {
    /// Create a registry for the configured table.
    pub fn new(config: &Config, hot_buffer: Arc<HotBuffer>) -> Self {
        Self {
            database: config.iceberg.database_name.clone(),
            table: config.iceberg.table_name.clone(),
            warehouse_path: config.iceberg.warehouse_path.clone(),
            hot_buffer,
            current_lsn: AtomicU64::new(0),
            next_scan_id: AtomicU64::new(1),
            next_flush_id: AtomicU64::new(1),
            data_files: RwLock::new(Vec::new()),
            inflight_flushes: RwLock::new(HashMap::new()),
            active_scans: RwLock::new(HashMap::new()),
            max_concurrent_scans: config.rpc.max_concurrent_scans,
            scan_ttl: Duration::from_secs(config.rpc.scan_ttl_seconds),
        }
    }

    /// Assign the next table-scoped read LSN.
    pub fn next_lsn(&self) -> u64 {
        self.current_lsn.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Return the latest assigned read LSN.
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn.load(Ordering::SeqCst)
    }

    /// List tables exposed by this K2I process.
    pub fn list_tables(&self) -> Vec<TableSummary> {
        vec![TableSummary {
            database: self.database.clone(),
            table: self.table.clone(),
            current_lsn: self.current_lsn(),
            data_file_count: self.data_files.read().len(),
        }]
    }

    /// Return the table schema encoded as an Arrow IPC stream.
    pub fn table_schema_ipc(&self, database: &str, table: &str) -> Result<Vec<u8>> {
        self.ensure_table(database, table)?;
        self.hot_buffer.schema_to_arrow_ipc()
    }

    /// Keep flushed records visible to readers while their Parquet file is being committed.
    pub fn begin_flush(&self, records: Vec<BufferedRecord>) -> Option<u64> {
        self.begin_flush_inner(records, None)
    }

    /// Keep flushed decoded Arrow rows visible while their file is being committed.
    pub fn begin_flush_batch(
        &self,
        records: Vec<BufferedRecord>,
        batch: RecordBatch,
    ) -> Option<u64> {
        self.begin_flush_inner(records, Some(batch))
    }

    fn begin_flush_inner(
        &self,
        records: Vec<BufferedRecord>,
        batch: Option<RecordBatch>,
    ) -> Option<u64> {
        if records.is_empty() {
            return None;
        }

        let flush_id = self.next_flush_id.fetch_add(1, Ordering::SeqCst);
        self.inflight_flushes
            .write()
            .insert(flush_id, InflightFlush { records, batch });
        Some(flush_id)
    }

    /// Remove an in-flight flush after it has been represented by a committed data file.
    pub fn complete_flush(&self, flush_id: u64) {
        self.inflight_flushes.write().remove(&flush_id);
    }

    /// Drop an in-flight flush when the write failed.
    pub fn abort_flush(&self, flush_id: u64) {
        self.inflight_flushes.write().remove(&flush_id);
    }

    /// Register a committed Iceberg/Parquet data file as visible to read clients.
    pub fn register_data_file(&self, file: RegisterDataFile) -> Result<DataFileRef> {
        self.observe_lsn(file.max_lsn);

        let data_file = DataFileRef {
            min_lsn: file.min_lsn,
            lsn: file.max_lsn,
            database: self.database.clone(),
            table: self.table.clone(),
            path: self.absolute_data_path(&file.file_path),
            size_bytes: file.size_bytes,
            row_count: file.row_count,
            topic: file.topic,
            partition: file.partition,
            min_offset: file.min_offset,
            max_offset: file.max_offset,
            snapshot_id: file.snapshot_id,
        };

        self.data_files.write().push(data_file.clone());
        Ok(data_file)
    }

    /// Restore data files recovered from the transaction log.
    pub fn restore_recovered_data_files(&self, files: &[RecoveredReadDataFile]) -> Result<usize> {
        let mut restored = 0;
        for file in files {
            if file.database != self.database || file.table != self.table {
                continue;
            }

            self.register_data_file(RegisterDataFile {
                file_path: file.file_path.clone(),
                size_bytes: file.file_size_bytes,
                row_count: file.row_count,
                topic: file.topic.clone(),
                partition: file.partition,
                min_offset: file.min_offset,
                max_offset: file.max_offset,
                min_lsn: file.min_lsn,
                max_lsn: file.read_lsn,
                snapshot_id: file.snapshot_id,
            })?;
            restored += 1;
        }

        Ok(restored)
    }

    /// Begin a consistent table scan.
    ///
    /// `lsn_bound = None` returns the latest assigned LSN. `Some(lsn)` returns
    /// an exact as-of scan at that LSN once the registry has reached it.
    pub fn begin_scan(
        &self,
        database: &str,
        table: &str,
        lsn_bound: Option<u64>,
    ) -> Result<ReadState> {
        self.ensure_table(database, table)?;
        self.expire_old_scans();

        let current_lsn = self.current_lsn();
        let scan_lsn = lsn_bound.unwrap_or(current_lsn);
        if let Some(bound) = lsn_bound {
            if current_lsn < bound {
                return Err(Error::ReadState(format!(
                    "requested LSN {} is ahead of current LSN {}",
                    bound, current_lsn
                )));
            }
        }

        let data_files: Vec<DataFileRef> = self
            .data_files
            .read()
            .iter()
            .filter(|file| file.min_lsn <= scan_lsn)
            .cloned()
            .collect();

        let mut decoded_batches = self
            .hot_buffer
            .snapshot_record_batches_for_read(Some(scan_lsn))?;
        let mut hot_records = self.hot_buffer.snapshot_records_for_read(Some(scan_lsn));
        for flush in self.inflight_flushes.read().values() {
            hot_records.extend(
                flush
                    .records
                    .iter()
                    .filter(|record| record.read_lsn <= scan_lsn)
                    .cloned(),
            );
            if let Some(batch) = &flush.batch {
                decoded_batches.push(crate::buffer::filter_batch_by_lsn(batch, Some(scan_lsn))?);
            }
        }
        hot_records.sort_by_key(|record| (record.read_lsn, record.partition, record.offset));

        let hot_arrow_ipc = if decoded_batches.is_empty() {
            self.hot_buffer.records_to_arrow_ipc(&hot_records)?
        } else {
            self.hot_buffer
                .record_batches_to_arrow_ipc(&decoded_batches)?
        };
        let partition_watermarks = Self::partition_watermarks(&data_files);

        let scan_id = self.next_scan_id.fetch_add(1, Ordering::SeqCst);
        {
            let mut active_scans = self.active_scans.write();
            if active_scans.len() >= self.max_concurrent_scans {
                return Err(Error::ReadState(format!(
                    "too many active scans: {} >= {}",
                    active_scans.len(),
                    self.max_concurrent_scans
                )));
            }

            active_scans.insert(
                scan_id,
                ActiveScan {
                    file_paths: data_files.iter().map(|file| file.path.clone()).collect(),
                    created_at: Instant::now(),
                },
            );
        }

        Ok(ReadState {
            scan_id,
            lsn: scan_lsn,
            database: self.database.clone(),
            table: self.table.clone(),
            data_files,
            hot_arrow_ipc,
            partition_watermarks,
            position_deletes: Vec::new(),
            deletion_vectors: Vec::new(),
            created_at: Utc::now(),
        })
    }

    /// End a scan and release pinned file references.
    pub fn end_scan(&self, database: &str, table: &str, scan_id: u64) -> Result<()> {
        self.ensure_table(database, table)?;
        self.active_scans.write().remove(&scan_id);
        Ok(())
    }

    /// Return true if a data file is referenced by an active scan.
    pub fn is_file_pinned(&self, path: &str) -> bool {
        self.expire_old_scans();
        self.active_scans
            .read()
            .values()
            .any(|scan| scan.file_paths.iter().any(|pinned| pinned == path))
    }

    fn ensure_table(&self, database: &str, table: &str) -> Result<()> {
        if database == self.database && table == self.table {
            return Ok(());
        }

        Err(Error::ReadState(format!(
            "unknown table {}.{}",
            database, table
        )))
    }

    fn observe_lsn(&self, lsn: u64) {
        let mut current = self.current_lsn();
        while lsn > current {
            match self.current_lsn.compare_exchange(
                current,
                lsn,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    fn absolute_data_path(&self, file_path: &str) -> String {
        if file_path.contains("://") || self.warehouse_path.contains("://") {
            return format!(
                "{}/{}",
                self.warehouse_path.trim_end_matches('/'),
                file_path.trim_start_matches('/')
            );
        }

        let path = Path::new(file_path);
        let full_path = if path.is_absolute() {
            PathBuf::from(path)
        } else {
            Path::new(&self.warehouse_path).join(path)
        };

        full_path
            .canonicalize()
            .unwrap_or(full_path)
            .to_string_lossy()
            .to_string()
    }

    fn expire_old_scans(&self) {
        let ttl = self.scan_ttl;
        self.active_scans
            .write()
            .retain(|_, scan| scan.created_at.elapsed() <= ttl);
    }

    fn partition_watermarks(data_files: &[DataFileRef]) -> Vec<PartitionWatermark> {
        let mut by_partition: HashMap<(String, i32), i64> = HashMap::new();
        for file in data_files {
            by_partition
                .entry((file.topic.clone(), file.partition))
                .and_modify(|offset| {
                    if file.max_offset > *offset {
                        *offset = file.max_offset;
                    }
                })
                .or_insert(file.max_offset);
        }

        let mut watermarks: Vec<PartitionWatermark> = by_partition
            .into_iter()
            .map(
                |((topic, partition), committed_offset)| PartitionWatermark {
                    topic,
                    partition,
                    committed_offset,
                },
            )
            .collect();
        watermarks.sort_by(|left, right| {
            left.topic
                .cmp(&right.topic)
                .then(left.partition.cmp(&right.partition))
        });
        watermarks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BufferConfig, RpcConfig};
    use crate::kafka::KafkaMessage;

    fn test_config() -> Config {
        Config {
            kafka: crate::config::KafkaConfig {
                bootstrap_servers: vec!["localhost:9092".into()],
                topic: "events".into(),
                consumer_group: "k2i".into(),
                batch_size: 10,
                batch_timeout_ms: 100,
                session_timeout_ms: 30_000,
                heartbeat_interval_ms: 3_000,
                max_poll_interval_ms: 300_000,
                auto_offset_reset: crate::config::OffsetReset::Earliest,
                security: crate::config::KafkaSecurityConfig::default(),
                format: crate::config::KafkaFormatConfig::Raw,
            },
            schema_evolution: crate::config::SchemaEvolutionRuntimeConfig::default(),
            iceberg: crate::config::IcebergConfig {
                catalog_type: crate::config::CatalogType::Rest,
                warehouse_path: "/tmp/k2i-test-warehouse".into(),
                database_name: "raw".into(),
                table_name: "events".into(),
                target_file_size_mb: 512,
                compression: crate::config::ParquetCompression::Snappy,
                partition_spec: vec![],
                rest_uri: None,
                hive_metastore_uri: None,
                aws_region: None,
                aws_access_key_id: None,
                aws_secret_access_key: None,
                s3_endpoint: None,
                catalog_manager: crate::config::CatalogManagerConfig::default(),
                table_management: crate::config::TableManagementConfig::default(),
                rest: crate::config::RestCatalogConfig::default(),
                glue: crate::config::GlueCatalogConfig::default(),
                nessie: None,
                sql_catalog: None,
                object_store: crate::config::ObjectStoreConfig::default(),
            },
            buffer: BufferConfig::default(),
            transaction_log: crate::config::TransactionLogConfig::default(),
            maintenance: crate::config::MaintenanceConfig::default(),
            monitoring: crate::config::MonitoringConfig::default(),
            rpc: RpcConfig::default(),
        }
    }

    fn message(offset: i64) -> KafkaMessage {
        KafkaMessage {
            key: Some(format!("key-{}", offset).into_bytes()),
            value: Some(format!("value-{}", offset).into_bytes()),
            topic: "events".to_string(),
            partition: 0,
            offset,
            timestamp: 1_700_000_000_000,
            headers: vec![],
        }
    }

    #[test]
    fn begin_scan_includes_hot_rows() {
        let config = test_config();
        let hot_buffer = Arc::new(HotBuffer::new(config.buffer.clone()));
        let registry = TableReadRegistry::new(&config, hot_buffer.clone());

        let lsn = registry.next_lsn();
        hot_buffer.append_with_lsn(&message(10), lsn).unwrap();

        let state = registry.begin_scan("raw", "events", None).unwrap();
        assert_eq!(state.lsn, 1);
        assert!(state.hot_arrow_ipc.is_some());
        assert!(state.data_files.is_empty());
        registry.end_scan("raw", "events", state.scan_id).unwrap();
    }

    #[test]
    fn inflight_flush_bridges_hot_cold_gap() {
        let config = test_config();
        let hot_buffer = Arc::new(HotBuffer::new(config.buffer.clone()));
        let registry = TableReadRegistry::new(&config, hot_buffer.clone());

        let lsn = registry.next_lsn();
        hot_buffer.append_with_lsn(&message(10), lsn).unwrap();
        let snapshot = hot_buffer.take_snapshot().unwrap().unwrap();
        let flush_id = registry.begin_flush(snapshot.records).unwrap();

        let state = registry.begin_scan("raw", "events", Some(lsn)).unwrap();
        assert!(state.hot_arrow_ipc.is_some());
        assert!(state.data_files.is_empty());
        registry.end_scan("raw", "events", state.scan_id).unwrap();

        let file = registry
            .register_data_file(RegisterDataFile {
                file_path: "data/raw/events/part-1.parquet".to_string(),
                size_bytes: 100,
                row_count: 1,
                topic: "events".to_string(),
                partition: 0,
                min_offset: 10,
                max_offset: 10,
                min_lsn: lsn,
                max_lsn: lsn,
                snapshot_id: 42,
            })
            .unwrap();
        registry.complete_flush(flush_id);

        let state = registry.begin_scan("raw", "events", Some(lsn)).unwrap();
        assert!(state.hot_arrow_ipc.is_none());
        assert_eq!(state.data_files, vec![file]);
        assert_eq!(state.partition_watermarks[0].committed_offset, 10);
    }

    #[test]
    fn lsn_bound_returns_exact_as_of_state() {
        let config = test_config();
        let hot_buffer = Arc::new(HotBuffer::new(config.buffer.clone()));
        let registry = TableReadRegistry::new(&config, hot_buffer.clone());

        let lsn_1 = registry.next_lsn();
        hot_buffer.append_with_lsn(&message(10), lsn_1).unwrap();
        let snapshot = hot_buffer.take_snapshot().unwrap().unwrap();
        let flush_id = registry.begin_flush(snapshot.records).unwrap();
        let file = registry
            .register_data_file(RegisterDataFile {
                file_path: "data/raw/events/part-1.parquet".to_string(),
                size_bytes: 100,
                row_count: 1,
                topic: "events".to_string(),
                partition: 0,
                min_offset: 10,
                max_offset: 10,
                min_lsn: lsn_1,
                max_lsn: lsn_1,
                snapshot_id: 42,
            })
            .unwrap();
        registry.complete_flush(flush_id);

        let lsn_2 = registry.next_lsn();
        hot_buffer.append_with_lsn(&message(11), lsn_2).unwrap();

        let state = registry.begin_scan("raw", "events", Some(lsn_1)).unwrap();
        assert_eq!(state.lsn, lsn_1);
        assert_eq!(state.data_files, vec![file]);
        assert!(state.hot_arrow_ipc.is_none());

        let latest = registry.begin_scan("raw", "events", None).unwrap();
        assert_eq!(latest.lsn, lsn_2);
        assert!(latest.hot_arrow_ipc.is_some());
    }

    #[test]
    fn as_of_scan_includes_overlapping_file_for_row_level_filtering() {
        let config = test_config();
        let hot_buffer = Arc::new(HotBuffer::new(config.buffer.clone()));
        let registry = TableReadRegistry::new(&config, hot_buffer);

        registry.observe_lsn(5);
        let file = registry
            .register_data_file(RegisterDataFile {
                file_path: "data/raw/events/part-overlap.parquet".to_string(),
                size_bytes: 200,
                row_count: 5,
                topic: "events".to_string(),
                partition: 0,
                min_offset: 10,
                max_offset: 14,
                min_lsn: 1,
                max_lsn: 5,
                snapshot_id: 43,
            })
            .unwrap();

        let state = registry.begin_scan("raw", "events", Some(3)).unwrap();
        assert_eq!(state.lsn, 3);
        assert_eq!(state.data_files, vec![file]);
    }

    #[test]
    fn lsn_bound_ahead_errors() {
        let config = test_config();
        let hot_buffer = Arc::new(HotBuffer::new(config.buffer.clone()));
        let registry = TableReadRegistry::new(&config, hot_buffer);

        let err = registry.begin_scan("raw", "events", Some(1)).unwrap_err();
        assert!(err.to_string().contains("ahead"));
    }
}
