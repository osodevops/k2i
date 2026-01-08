//! Main ingestion engine orchestration.
//!
//! Integrates all components for Kafka to Iceberg ingestion:
//! - Kafka consumer with backpressure support
//! - Hot buffer for in-memory data
//! - Iceberg writer with catalog integration
//! - Transaction log for crash recovery
//! - Maintenance scheduler for compaction and cleanup

use crate::buffer::HotBuffer;
use crate::config::Config;
use crate::health::HealthCheck;
use crate::iceberg::{
    create_shared_cache, CacheStats, CatalogFactoryRegistry, CatalogOperations, IcebergWriter,
    IcebergWriterBuilder, SharedMetadataCache, TransactionCoordinator,
    TransactionCoordinatorBuilder, TransactionStats,
};
use crate::kafka::{KafkaConsumerBuilder, SmartKafkaConsumer};
use crate::maintenance::MaintenanceScheduler;
use crate::metrics::IngestionMetrics;
use crate::txlog::{RecoveryState, TransactionEntry, TransactionLog};
use crate::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

/// Main ingestion engine.
pub struct IngestionEngine {
    config: Config,
    consumer: SmartKafkaConsumer,
    hot_buffer: Arc<HotBuffer>,
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

        // Initialize hot buffer
        let hot_buffer = Arc::new(HotBuffer::new(config.buffer.clone()));

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

        Ok(Self {
            config,
            consumer,
            hot_buffer,
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
        })
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

        for msg in messages {
            // Append to hot buffer
            self.hot_buffer.append(&msg)?;

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

    async fn flush_buffer(&mut self) -> Result<()> {
        let snapshot = self.hot_buffer.take_snapshot()?;

        if let Some(batch) = snapshot {
            let row_count = batch.num_rows();
            let start = std::time::Instant::now();

            // Get the min and max offset from the batch
            let (min_offset, max_offset) = batch
                .column_by_name("offset")
                .and_then(|c| c.as_any().downcast_ref::<arrow::array::Int64Array>())
                .map(|arr| {
                    if arr.is_empty() {
                        (0, 0)
                    } else {
                        (arr.value(0), arr.value(arr.len() - 1))
                    }
                })
                .unwrap_or((0, 0));

            // Write to Iceberg using the configured writer
            let (snapshot_id, file_path) = if let Some(ref mut writer) = self.iceberg_writer {
                match writer.write_batch(batch, max_offset).await {
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
                        (stats.snapshot_id, stats.file_path.clone())
                    }
                    Err(e) => {
                        self.health
                            .mark_unhealthy("iceberg", &format!("Write failed: {}", e));
                        return Err(e);
                    }
                }
            } else {
                // No writer configured, use mock commit
                warn!("No Iceberg writer configured, using mock commit");
                (chrono::Utc::now().timestamp(), String::new())
            };

            // Log IdempotencyRecord for exactly-once semantics
            // This record enables deduplication during crash recovery
            let topic = self.config.kafka.topic.clone();
            self.txlog.append(TransactionEntry::IdempotencyRecord {
                kafka_offset_min: min_offset,
                kafka_offset_max: max_offset,
                topic: topic.clone(),
                partition: 0, // TODO: Support multiple partitions
                snapshot_id,
                file_paths: if file_path.is_empty() {
                    vec![]
                } else {
                    vec![file_path]
                },
                committed_at: chrono::Utc::now(),
            })?;

            // Commit Kafka offset (only after successful Iceberg write AND idempotency record)
            if let Err(e) = self.consumer.commit_offset(&topic, 0, max_offset).await {
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
    pub fn metrics(&self) -> &IngestionMetrics {
        &self.metrics
    }

    /// Get health check.
    pub fn health(&self) -> &HealthCheck {
        &self.health
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
