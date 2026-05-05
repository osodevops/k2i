//! Main ingestion engine orchestration.
//!
//! Integrates all components for Kafka to Iceberg ingestion:
//! - Kafka consumer with backpressure support
//! - Hot buffer for in-memory data
//! - Iceberg writer with catalog integration
//! - Transaction log for crash recovery
//! - Maintenance scheduler for compaction and cleanup

use crate::buffer::HotBuffer;
use crate::config::{Config, KafkaFormatConfig, OnBreakingChange, SchemaEvolutionMode};
use crate::format::protobuf::diff_table_schema;
use crate::format::protobuf::registry::HttpSchemaRegistryClient;
use crate::format::protobuf::ProtobufDecoder;
use crate::format::{DecodedBatch, MessageDecoder, RawDecoder};
use crate::health::HealthCheck;
use crate::iceberg::{
    create_shared_cache, CacheStats, CachedField, CachedSchema, CatalogFactoryRegistry,
    CatalogOperations, IcebergWriter, IcebergWriterBuilder, SchemaFieldInfo, SharedMetadataCache,
    TableSchema, TransactionCoordinator, TransactionCoordinatorBuilder, TransactionStats,
};
use crate::kafka::{KafkaConsumerBuilder, SmartKafkaConsumer};
use crate::maintenance::MaintenanceScheduler;
use crate::metrics::IngestionMetrics;
use crate::read::{RegisterDataFile, TableReadRegistry};
use crate::txlog::{RecoveryState, SchemaField, TransactionEntry, TransactionLog};
use crate::{Error, IcebergError, Result};
use arrow::array::UInt64Array;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

fn evolved_table_schema(
    existing: &TableSchema,
    additive_fields: &[SchemaFieldInfo],
    incoming: &TableSchema,
) -> TableSchema {
    let mut fields = existing.fields.clone();
    let next_id = fields.iter().map(|field| field.id).max().unwrap_or(0) + 1;

    fields.extend(
        additive_fields
            .iter()
            .enumerate()
            .map(|(idx, field)| SchemaFieldInfo {
                id: next_id + idx as i32,
                name: field.name.clone(),
                field_type: field.field_type.clone(),
                required: false,
                doc: field.doc.clone(),
            }),
    );

    TableSchema {
        schema_id: incoming.schema_id,
        fields,
    }
}

fn with_read_lsns(batch: RecordBatch, read_lsns: &[u64]) -> Result<RecordBatch> {
    if batch.num_rows() != read_lsns.len() {
        return Err(Error::Buffer(crate::BufferError::SchemaMismatch {
            expected: format!("{} read LSNs", batch.num_rows()),
            actual: format!("{} read LSNs", read_lsns.len()),
        }));
    }

    let read_lsn_idx = batch
        .schema()
        .index_of(crate::format::COL_READ_LSN)
        .map_err(|e| {
            Error::Buffer(crate::BufferError::SchemaMismatch {
                expected: crate::format::COL_READ_LSN.to_string(),
                actual: e.to_string(),
            })
        })?;

    let mut columns = batch.columns().to_vec();
    columns[read_lsn_idx] = Arc::new(UInt64Array::from(read_lsns.to_vec()));

    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|e| Error::Buffer(crate::BufferError::ArrowConversion(e.to_string())))
}

/// Main ingestion engine.
pub struct IngestionEngine {
    config: Config,
    consumer: SmartKafkaConsumer,
    decoder: Arc<dyn MessageDecoder>,
    hot_buffer: Arc<HotBuffer>,
    read_registry: Arc<TableReadRegistry>,
    txlog: Arc<TransactionLog>,
    #[allow(dead_code)]
    maintenance: Arc<MaintenanceScheduler>,
    metrics: Arc<IngestionMetrics>,
    health: Arc<HealthCheck>,
    shutdown_tx: broadcast::Sender<()>,
    /// Iceberg writer for writing to Iceberg tables
    iceberg_writer: Option<IcebergWriter>,
    /// Catalog operations for table management
    catalog: Option<Arc<dyn CatalogOperations>>,
    /// Metadata cache for performance
    metadata_cache: SharedMetadataCache,
    /// Transaction coordinator for atomic commits
    transaction_coordinator: Option<Arc<TransactionCoordinator>>,
    /// Recovery state from transaction log (for offset tracking)
    recovery_state: RecoveryState,
    /// Last decoded structured table schema observed by the ingest path.
    current_table_schema: Option<TableSchema>,
    /// Last successful schema update commit, used to throttle rapid schema drift.
    last_schema_update_at: Option<Instant>,
    /// True when ingestion is intentionally paused until operator intervention.
    paused_for_schema_change: bool,
}

impl IngestionEngine {
    /// Create a new ingestion engine.
    pub async fn new(config: Config) -> Result<Self> {
        config.validate()?;

        // Initialize transaction log first (for recovery)
        let txlog = Arc::new(TransactionLog::open(config.transaction_log.clone())?);

        // Perform recovery from transaction log
        let recovery_state = RecoveryState::recover_from(&txlog)?;

        // Log recovery summary
        let summary = recovery_state.summary();
        if summary.entries_processed > 0 {
            info!(
                entries_processed = summary.entries_processed,
                last_snapshot = ?summary.last_iceberg_snapshot,
                max_kafka_offset = ?summary.max_kafka_offset,
                incomplete_flushes = summary.incomplete_flush_count,
                orphan_files = summary.orphan_file_count,
                "Recovery complete"
            );
        }

        // Warn if there are orphan files that need cleanup
        if recovery_state.needs_cleanup() {
            warn!(
                incomplete_flushes = summary.incomplete_flush_count,
                orphan_files = summary.orphan_file_count,
                orphan_bytes = summary.orphan_file_size_bytes,
                "Found orphan files from previous crash - will clean up when object store is available"
            );
        }

        // Initialize Kafka consumer
        let consumer = KafkaConsumerBuilder::new(config.kafka.clone()).build()?;
        let (decoder, startup_table_schema) = Self::initialize_decoder(&config).await?;

        // Initialize hot buffer
        let hot_buffer = Arc::new(HotBuffer::new(config.buffer.clone()));
        let read_registry = Arc::new(TableReadRegistry::new(&config, hot_buffer.clone()));
        let restored_read_files =
            read_registry.restore_recovered_data_files(&recovery_state.read_data_files)?;
        if restored_read_files > 0 {
            info!(
                files = restored_read_files,
                "Restored committed data files into read-state registry"
            );
        }

        // Initialize maintenance scheduler
        let maintenance = Arc::new(MaintenanceScheduler::new(config.maintenance.clone()));

        // Initialize metrics
        let metrics = Arc::new(IngestionMetrics::new());

        // Initialize health check
        let health = Arc::new(HealthCheck::new());
        health.register_component("kafka");
        health.register_component("buffer");
        health.register_component("iceberg");
        health.register_component("catalog");
        health.register_component("txlog");
        health.register_component("schema");
        health.mark_healthy("txlog");

        // Initialize metadata cache
        let metadata_cache = create_shared_cache();

        // Initialize catalog integration if configured
        let (catalog, transaction_coordinator, iceberg_writer) =
            Self::initialize_catalog_components(&config, txlog.clone(), metadata_cache.clone())
                .await?;

        // Update health status for catalog
        if catalog.is_some() {
            health.mark_healthy("catalog");
            info!("Catalog integration initialized");
        } else {
            health.mark_degraded("catalog", "No catalog configured, using mock commits");
            debug!("Running without catalog integration");
        }

        let (shutdown_tx, _) = broadcast::channel(1);

        let current_table_schema = Self::initialize_startup_schema(
            &config,
            catalog.as_ref(),
            &health,
            startup_table_schema,
        )
        .await?;

        Ok(Self {
            config,
            consumer,
            decoder,
            hot_buffer,
            read_registry,
            txlog,
            maintenance,
            metrics,
            health,
            shutdown_tx,
            iceberg_writer,
            catalog,
            metadata_cache,
            transaction_coordinator,
            recovery_state,
            current_table_schema,
            last_schema_update_at: None,
            paused_for_schema_change: false,
        })
    }

    async fn initialize_decoder(
        config: &Config,
    ) -> Result<(Arc<dyn MessageDecoder>, Option<TableSchema>)> {
        match &config.kafka.format {
            KafkaFormatConfig::Raw | KafkaFormatConfig::Json => Ok((Arc::new(RawDecoder), None)),
            KafkaFormatConfig::Protobuf(protobuf_config) => {
                let registry = Arc::new(HttpSchemaRegistryClient::with_disk_cache(
                    protobuf_config.schema_registry_url.clone(),
                    Duration::from_secs(protobuf_config.cache_ttl_seconds),
                    config.transaction_log.log_dir.join("schema-cache"),
                ));
                let decoder = Arc::new(ProtobufDecoder::new(registry, protobuf_config.clone()));
                let startup_table_schema = if protobuf_config.latest_on_startup {
                    Some(
                        decoder
                            .resolve_latest_table_schema(&config.kafka.topic)
                            .await?,
                    )
                } else {
                    None
                };
                Ok((decoder, startup_table_schema))
            }
        }
    }

    async fn initialize_startup_schema(
        config: &Config,
        catalog: Option<&Arc<dyn CatalogOperations>>,
        health: &Arc<HealthCheck>,
        startup_table_schema: Option<TableSchema>,
    ) -> Result<Option<TableSchema>> {
        let Some(incoming_schema) = startup_table_schema else {
            return Ok(None);
        };

        let Some(catalog) = catalog else {
            health.mark_healthy("schema");
            return Ok(Some(incoming_schema));
        };

        let namespace = &config.iceberg.database_name;
        let table = &config.iceberg.table_name;
        if !catalog.namespace_exists(namespace).await? {
            catalog.create_namespace(namespace).await?;
        }

        if !catalog.table_exists(namespace, table).await? {
            let table_info = catalog
                .create_table(namespace, table, &incoming_schema)
                .await?;
            health.mark_healthy("schema");
            return Ok(Some(table_info.schema));
        }

        let table_info = catalog.load_table(namespace, table).await?;
        let diff = diff_table_schema(&table_info.schema, &incoming_schema);
        if diff.is_empty() {
            health.mark_healthy("schema");
            return Ok(Some(table_info.schema));
        }

        if !diff.breaking_changes.is_empty() {
            let reason = format!(
                "startup protobuf schema {} is incompatible with table schema {}: {}",
                incoming_schema.schema_id,
                table_info.schema.schema_id,
                diff.breaking_changes.join("; ")
            );
            health.mark_degraded("schema", &reason);
            health.block_readiness("schema", &reason);
            return Err(Error::Iceberg(IcebergError::SchemaEvolution(reason)));
        }

        if config.schema_evolution.mode == SchemaEvolutionMode::Manual {
            let reason = format!(
                "startup protobuf schema {} adds fields {:?}, but schema_evolution.mode=manual",
                incoming_schema.schema_id,
                diff.additive_fields
                    .iter()
                    .map(|field| field.name.as_str())
                    .collect::<Vec<_>>()
            );
            health.mark_degraded("schema", &reason);
            health.block_readiness("schema", &reason);
            return Err(Error::Iceberg(IcebergError::SchemaEvolution(reason)));
        }

        let evolved_schema =
            evolved_table_schema(&table_info.schema, &diff.additive_fields, &incoming_schema);
        let updated = catalog
            .update_schema(
                namespace,
                table,
                &evolved_schema,
                Some(table_info.schema.schema_id),
            )
            .await?;
        health.mark_healthy("schema");
        Ok(Some(updated.schema))
    }

    /// Initialize catalog components based on configuration.
    async fn initialize_catalog_components(
        config: &Config,
        txlog: Arc<TransactionLog>,
        metadata_cache: SharedMetadataCache,
    ) -> Result<(
        Option<Arc<dyn CatalogOperations>>,
        Option<Arc<TransactionCoordinator>>,
        Option<IcebergWriter>,
    )> {
        // Check if we have a REST URI configured (indicating real catalog usage)
        if config.iceberg.rest_uri.is_none() {
            // No catalog configured, return writer without catalog
            let writer = IcebergWriterBuilder::new(config.iceberg.clone())
                .with_txlog(txlog)
                .with_metadata_cache(metadata_cache)
                .build()
                .await?;

            return Ok((None, None, Some(writer)));
        }

        // Create catalog factory registry and create the catalog client
        let registry = CatalogFactoryRegistry::with_defaults();
        let catalog: Arc<dyn CatalogOperations> = registry.create(&config.iceberg).await?;

        // Perform initial health check
        let health = catalog.health_check().await?;
        if !health.is_healthy {
            warn!(
                catalog_type = ?config.iceberg.catalog_type,
                message = ?health.message,
                "Catalog health check failed, continuing anyway"
            );
        } else {
            info!(
                catalog_type = ?config.iceberg.catalog_type,
                response_time_ms = health.response_time_ms,
                "Catalog health check passed"
            );
        }

        // Create transaction coordinator
        let coordinator = Arc::new(
            TransactionCoordinatorBuilder::new()
                .catalog(catalog.clone())
                .namespace(config.iceberg.database_name.clone())
                .table_name(config.iceberg.table_name.clone())
                .txlog(txlog.clone())
                .max_retries(config.iceberg.catalog_manager.max_retries)
                .enable_idempotency(true)
                .build()?,
        );

        // Create writer with full catalog integration
        let writer = IcebergWriterBuilder::new(config.iceberg.clone())
            .with_txlog(txlog.clone())
            .with_catalog(catalog.clone())
            .with_transaction_coordinator(coordinator.clone())
            .with_metadata_cache(metadata_cache)
            .build()
            .await?;

        Ok((Some(catalog), Some(coordinator), Some(writer)))
    }

    /// Run the main ingestion loop.
    pub async fn run(&mut self) -> Result<()> {
        self.health.job_started();
        info!("Ingestion engine started");

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received");
                    break;
                }

                result = self.process_batch() => {
                    if let Err(e) = result {
                        error!(error = %e, "Error processing batch");
                        self.metrics.record_error();

                        if !self.health.is_operational() {
                            error!("System unhealthy, stopping ingestion");
                            break;
                        }
                    }
                }
            }
        }

        // Graceful shutdown
        self.shutdown().await?;
        self.health.job_completed();

        Ok(())
    }

    async fn process_batch(&mut self) -> Result<()> {
        if self.paused_for_schema_change {
            tokio::time::sleep(Duration::from_secs(1)).await;
            return Ok(());
        }

        // Check backpressure
        if self.hot_buffer.is_full() {
            if !self.consumer.is_paused() {
                self.consumer.pause()?;
                self.metrics.record_backpressure();
                self.health
                    .mark_degraded("buffer", "Buffer full, backpressure active");
            }
            // Trigger immediate flush
            self.flush_buffer().await?;
            self.consumer.resume()?;
            self.health.mark_healthy("buffer");
            return Ok(());
        }

        // Poll for messages
        let timeout = Duration::from_millis(self.config.kafka.batch_timeout_ms);
        let messages = match self
            .consumer
            .poll_batch(self.config.kafka.batch_size, timeout)
            .await
        {
            Ok(msgs) => {
                self.health.mark_healthy("kafka");
                msgs
            }
            Err(e) => {
                self.health
                    .mark_unhealthy("kafka", &format!("Poll failed: {}", e));
                return Err(e);
            }
        };

        if messages.is_empty() {
            return Ok(());
        }

        let provisional_lsns = vec![0; messages.len()];
        let mut decoded = self
            .decoder
            .decode_batch(&messages, &provisional_lsns)
            .await?;
        self.handle_schema_evolution(&decoded).await?;
        let read_lsns: Vec<u64> = messages
            .iter()
            .map(|_| self.read_registry.next_lsn())
            .collect();
        decoded.batch = with_read_lsns(decoded.batch, &read_lsns)?;
        self.hot_buffer
            .append_record_batch(decoded.batch, &messages, &read_lsns)?;

        for msg in messages {
            // Record offset in transaction log
            self.txlog.append(TransactionEntry::OffsetMarker {
                topic: msg.topic.clone(),
                partition: msg.partition,
                offset: msg.offset,
                record_count: 1,
                timestamp: chrono::Utc::now(),
            })?;

            self.metrics.record_message();
        }

        // Update buffer health
        self.health.mark_healthy("buffer");

        // Check if we should flush
        if self.hot_buffer.should_flush() {
            self.flush_buffer().await?;
        }

        Ok(())
    }

    async fn handle_schema_evolution(&mut self, decoded: &DecodedBatch) -> Result<()> {
        let Some(incoming_schema) = decoded.table_schema.as_ref() else {
            return Ok(());
        };

        let Some(existing_schema) = self.current_table_schema.clone() else {
            self.current_table_schema = Some(incoming_schema.clone());
            self.health.mark_healthy("schema");
            return Ok(());
        };

        let diff = diff_table_schema(&existing_schema, incoming_schema);
        if diff.is_empty() {
            self.health.mark_healthy("schema");
            return Ok(());
        }

        if !diff.breaking_changes.is_empty() {
            let reason = format!(
                "breaking schema change for IDs {:?}: {}",
                decoded.schema_ids,
                diff.breaking_changes.join("; ")
            );

            return match self.config.schema_evolution.on_breaking_change {
                OnBreakingChange::Pause => {
                    self.pause_for_schema_change(&reason)?;
                    Err(Error::Iceberg(IcebergError::SchemaEvolution(reason)))
                }
                OnBreakingChange::Fail => {
                    self.health.mark_unhealthy("schema", &reason);
                    self.health.block_readiness("schema", &reason);
                    Err(Error::Iceberg(IcebergError::SchemaEvolution(reason)))
                }
                OnBreakingChange::SkipMessage => {
                    let reason = format!(
                        "{}; skip-message policy is not implemented because it would advance offsets past incompatible data",
                        reason
                    );
                    self.pause_for_schema_change(&reason)?;
                    Err(Error::Iceberg(IcebergError::SchemaEvolution(reason)))
                }
            };
        }

        if !diff.additive_fields.is_empty() {
            if self.config.schema_evolution.mode == SchemaEvolutionMode::Manual {
                let reason = format!(
                    "schema IDs {:?} add fields {:?}, but schema_evolution.mode=manual",
                    decoded.schema_ids,
                    diff.additive_fields
                        .iter()
                        .map(|field| field.name.as_str())
                        .collect::<Vec<_>>()
                );
                self.pause_for_schema_change(&reason)?;
                return Err(Error::Iceberg(IcebergError::SchemaEvolution(reason)));
            }

            if self.hot_buffer.row_count() > 0 {
                self.flush_buffer().await?;
            }

            self.wait_for_schema_update_budget().await;

            let evolved_schema =
                evolved_table_schema(&existing_schema, &diff.additive_fields, incoming_schema);

            if let Some(catalog) = &self.catalog {
                let table = catalog
                    .update_schema(
                        &self.config.iceberg.database_name,
                        &self.config.iceberg.table_name,
                        &evolved_schema,
                        Some(existing_schema.schema_id),
                    )
                    .await?;
                self.cache_table_schema(&table.schema);
                self.health.mark_healthy("catalog");
            }

            self.txlog.append(TransactionEntry::SchemaEvolution {
                database: self.config.iceberg.database_name.clone(),
                table: self.config.iceberg.table_name.clone(),
                fields_added: diff
                    .additive_fields
                    .iter()
                    .map(|field| SchemaField {
                        name: field.name.clone(),
                        field_type: field.field_type.clone(),
                        required: field.required,
                        doc: field.doc.clone(),
                    })
                    .collect(),
                fields_removed: Vec::new(),
                old_schema_id: Some(existing_schema.schema_id),
                new_schema_id: evolved_schema.schema_id,
                reason: format!("auto-additive protobuf schema IDs {:?}", decoded.schema_ids),
                timestamp: chrono::Utc::now(),
            })?;

            self.current_table_schema = Some(evolved_schema);
            self.last_schema_update_at = Some(Instant::now());
            self.health.mark_healthy("schema");
        }

        Ok(())
    }

    fn pause_for_schema_change(&mut self, reason: &str) -> Result<()> {
        self.consumer.pause()?;
        self.paused_for_schema_change = true;
        self.health.mark_degraded("schema", reason);
        self.health.block_readiness("schema", reason);
        warn!(reason = %reason, "Ingestion paused for schema change");
        Ok(())
    }

    async fn wait_for_schema_update_budget(&self) {
        let Some(last_schema_update_at) = self.last_schema_update_at else {
            return;
        };
        let min_interval = Duration::from_secs(
            self.config
                .schema_evolution
                .schema_update_min_interval_seconds,
        );
        let elapsed = last_schema_update_at.elapsed();
        if elapsed < min_interval {
            let wait = min_interval - elapsed;
            warn!(
                wait_ms = wait.as_millis(),
                "Throttling schema update to respect schema_update_min_interval_seconds"
            );
            tokio::time::sleep(wait).await;
        }
    }

    fn cache_table_schema(&self, schema: &TableSchema) {
        self.metadata_cache.set_schema(CachedSchema {
            schema_id: schema.schema_id,
            fields: schema
                .fields
                .iter()
                .map(|field| CachedField {
                    id: field.id,
                    name: field.name.clone(),
                    field_type: field.field_type.clone(),
                    required: field.required,
                })
                .collect(),
        });
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        let snapshot = self.hot_buffer.take_snapshot()?;

        if let Some(snapshot) = snapshot {
            let row_count = snapshot.batch.num_rows();
            let start = std::time::Instant::now();
            let flush_id = self
                .read_registry
                .begin_flush_batch(snapshot.records.clone(), snapshot.batch.clone());

            // Get the min and max offset from the batch
            let (min_offset, max_offset) = snapshot
                .batch
                .column_by_name("offset")
                .and_then(|c| c.as_any().downcast_ref::<arrow::array::Int64Array>())
                .map(|arr| {
                    if arr.is_empty() {
                        (0, 0)
                    } else {
                        let min = (0..arr.len()).map(|idx| arr.value(idx)).min().unwrap_or(0);
                        let max = (0..arr.len()).map(|idx| arr.value(idx)).max().unwrap_or(0);
                        (min, max)
                    }
                })
                .unwrap_or((0, 0));

            // Write to Iceberg using the configured writer
            let write_stats = if let Some(ref mut writer) = self.iceberg_writer {
                match writer.write_batch(snapshot.batch, max_offset).await {
                    Ok(stats) => {
                        self.health.mark_healthy("iceberg");
                        if writer.has_catalog_integration() {
                            self.health.mark_healthy("catalog");
                        }
                        debug!(
                            file_path = %stats.file_path,
                            file_size_bytes = stats.file_size_bytes,
                            parquet_ms = %stats.parquet_conversion_duration.as_millis(),
                            upload_ms = %stats.upload_duration.as_millis(),
                            commit_ms = %stats.commit_duration.as_millis(),
                            "Iceberg write completed"
                        );
                        Some(stats)
                    }
                    Err(e) => {
                        if let Some(flush_id) = flush_id {
                            self.read_registry.abort_flush(flush_id);
                        }
                        self.health
                            .mark_unhealthy("iceberg", &format!("Write failed: {}", e));
                        return Err(e);
                    }
                }
            } else {
                // No writer configured, use mock commit
                warn!("No Iceberg writer configured, using mock commit");
                None
            };

            let snapshot_id = write_stats
                .as_ref()
                .map(|stats| stats.snapshot_id)
                .unwrap_or_else(|| chrono::Utc::now().timestamp());
            let topic = write_stats
                .as_ref()
                .map(|stats| stats.topic.clone())
                .unwrap_or_else(|| self.config.kafka.topic.clone());
            let partition = write_stats
                .as_ref()
                .map(|stats| stats.kafka_partition)
                .unwrap_or(0);
            let min_offset = write_stats
                .as_ref()
                .map(|stats| stats.min_offset)
                .unwrap_or(min_offset);
            let max_offset = write_stats
                .as_ref()
                .map(|stats| stats.max_offset)
                .unwrap_or(max_offset);
            let mut committed_file_path = String::new();

            if let Some(ref stats) = write_stats {
                let data_file = self.read_registry.register_data_file(RegisterDataFile {
                    file_path: stats.file_path.clone(),
                    size_bytes: stats.file_size_bytes as u64,
                    row_count: stats.row_count as u64,
                    topic: stats.topic.clone(),
                    partition: stats.kafka_partition,
                    min_offset: stats.min_offset,
                    max_offset: stats.max_offset,
                    min_lsn: stats.min_lsn,
                    max_lsn: stats.max_lsn,
                    snapshot_id: stats.snapshot_id,
                })?;
                committed_file_path = data_file.path.clone();

                if let Some(flush_id) = flush_id {
                    self.read_registry.complete_flush(flush_id);
                }

                self.txlog.append(TransactionEntry::DataFileCommitted {
                    min_lsn: data_file.min_lsn,
                    read_lsn: data_file.lsn,
                    database: data_file.database.clone(),
                    table: data_file.table.clone(),
                    topic: data_file.topic.clone(),
                    partition: data_file.partition,
                    min_offset: data_file.min_offset,
                    max_offset: data_file.max_offset,
                    snapshot_id: data_file.snapshot_id,
                    file_path: data_file.path,
                    file_size_bytes: data_file.size_bytes,
                    row_count: data_file.row_count,
                    timestamp: chrono::Utc::now(),
                })?;
            } else if let Some(flush_id) = flush_id {
                self.read_registry.complete_flush(flush_id);
            }

            // Log IdempotencyRecord for exactly-once semantics
            // This record enables deduplication during crash recovery
            self.txlog.append(TransactionEntry::IdempotencyRecord {
                kafka_offset_min: min_offset,
                kafka_offset_max: max_offset,
                topic: topic.clone(),
                partition,
                snapshot_id,
                file_paths: if committed_file_path.is_empty() {
                    vec![]
                } else {
                    vec![committed_file_path]
                },
                committed_at: chrono::Utc::now(),
            })?;

            // Commit Kafka offset (only after successful Iceberg write AND idempotency record)
            if let Err(e) = self
                .consumer
                .commit_offset(&topic, partition, max_offset)
                .await
            {
                self.health
                    .mark_unhealthy("kafka", &format!("Offset commit failed: {}", e));
                return Err(e);
            }

            let duration = start.elapsed();
            self.metrics.record_flush(row_count, duration);

            info!(
                rows = %row_count,
                snapshot_id = %snapshot_id,
                offset_range = %format!("{}..{}", min_offset, max_offset),
                duration_ms = %duration.as_millis(),
                has_catalog = %self.catalog.is_some(),
                "Buffer flushed to Iceberg"
            );
        }

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down ingestion engine (30s timeout)");

        // Use timeout for graceful shutdown
        let shutdown_result =
            tokio::time::timeout(Duration::from_secs(30), self.graceful_shutdown_inner()).await;

        match shutdown_result {
            Ok(Ok(())) => {
                info!("Graceful shutdown completed successfully");
            }
            Ok(Err(e)) => {
                warn!(error = %e, "Shutdown completed with errors");
            }
            Err(_) => {
                warn!("Shutdown timeout exceeded (30s), forcing exit");
                // Force checkpoint before exit
                if let Err(e) = self.txlog.force_checkpoint() {
                    error!(error = %e, "Failed to force checkpoint during timeout");
                }
            }
        }

        Ok(())
    }

    async fn graceful_shutdown_inner(&mut self) -> Result<()> {
        // Final flush of any remaining data
        if let Err(e) = self.flush_buffer().await {
            warn!(error = %e, "Error during final buffer flush");
        }

        // Stop maintenance tasks
        self.maintenance.stop().await;

        // Force a final checkpoint
        self.txlog.force_checkpoint()?;

        info!("Ingestion engine shut down complete");
        Ok(())
    }

    /// Get metrics.
    pub fn metrics(&self) -> Arc<IngestionMetrics> {
        self.metrics.clone()
    }

    /// Get health check.
    pub fn health(&self) -> Arc<HealthCheck> {
        self.health.clone()
    }

    /// Get the read-state registry.
    pub fn read_registry(&self) -> Arc<TableReadRegistry> {
        self.read_registry.clone()
    }

    /// Get shutdown signal sender.
    pub fn shutdown_signal(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }

    /// Check if catalog integration is enabled.
    pub fn has_catalog_integration(&self) -> bool {
        self.catalog.is_some()
    }

    /// Get catalog operations if configured.
    pub fn catalog(&self) -> Option<&Arc<dyn CatalogOperations>> {
        self.catalog.as_ref()
    }

    /// Get transaction coordinator if configured.
    pub fn transaction_coordinator(&self) -> Option<&Arc<TransactionCoordinator>> {
        self.transaction_coordinator.as_ref()
    }

    /// Get metadata cache.
    pub fn metadata_cache(&self) -> &SharedMetadataCache {
        &self.metadata_cache
    }

    /// Get cache statistics.
    pub fn cache_stats(&self) -> &CacheStats {
        self.metadata_cache.stats()
    }

    /// Get transaction statistics if coordinator is configured.
    pub fn transaction_stats(&self) -> Option<&TransactionStats> {
        self.transaction_coordinator.as_ref().map(|c| c.stats())
    }

    /// Get the recovery state.
    pub fn recovery_state(&self) -> &RecoveryState {
        &self.recovery_state
    }

    /// Get the recovery summary for logging/reporting.
    pub fn recovery_summary(&self) -> crate::txlog::RecoverySummary {
        self.recovery_state.summary()
    }

    /// Check if a message at the given offset has already been processed.
    ///
    /// This is useful for deduplication after crash recovery.
    pub fn is_message_already_processed(&self, topic: &str, partition: i32, offset: i64) -> bool {
        self.recovery_state
            .is_already_processed(topic, partition, offset)
    }

    /// Get the starting offset for a partition after recovery.
    ///
    /// Returns the next offset to consume (last committed + 1).
    pub fn starting_offset_for(&self, topic: &str, partition: i32) -> Option<i64> {
        self.recovery_state.starting_offset_for(topic, partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(schema_id: i32, fields: Vec<SchemaFieldInfo>) -> TableSchema {
        TableSchema { schema_id, fields }
    }

    fn field(id: i32, name: &str, field_type: &str, required: bool) -> SchemaFieldInfo {
        SchemaFieldInfo {
            id,
            name: name.to_string(),
            field_type: field_type.to_string(),
            required,
            doc: None,
        }
    }

    #[test]
    fn evolved_schema_preserves_existing_field_ids_and_adds_nullable_fields() {
        let existing = schema(
            101,
            vec![
                field(10, "driver", "string", false),
                field(11, "lap", "int", false),
            ],
        );
        let incoming = schema(
            102,
            vec![
                field(1, "driver", "string", false),
                field(2, "lap", "int", false),
                field(3, "team", "string", false),
            ],
        );

        let evolved =
            evolved_table_schema(&existing, &[field(3, "team", "string", true)], &incoming);

        assert_eq!(evolved.schema_id, 102);
        assert_eq!(evolved.fields[0].id, 10);
        assert_eq!(evolved.fields[1].id, 11);
        assert_eq!(evolved.fields[2].id, 12);
        assert_eq!(evolved.fields[2].name, "team");
        assert!(!evolved.fields[2].required);
    }
}
