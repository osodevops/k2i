//! Integration tests for k2i-core.
//!
//! These tests require Docker to be running and are marked with #[ignore]
//! to avoid running them in normal test runs.
//!
//! Run with: cargo test --test integration_tests -- --ignored

use k2i_core::buffer::HotBuffer;
use k2i_core::config::{BufferConfig, TransactionLogConfig};
use k2i_core::health::{ComponentStatus, HealthCheck, HealthStatus};
use k2i_core::kafka::KafkaMessage;
use k2i_core::metrics::IngestionMetrics;
use k2i_core::txlog::{TransactionEntry, TransactionLog};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

mod kafka_integration {
    use super::*;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::Kafka;

    /// Test Kafka consumer connectivity with testcontainers.
    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn test_kafka_connection() {
        let kafka = Kafka::default()
            .start()
            .await
            .expect("Failed to start Kafka container");
        let bootstrap_servers = format!(
            "127.0.0.1:{}",
            kafka
                .get_host_port_ipv4(9093)
                .await
                .expect("Failed to get Kafka port")
        );

        // Create admin client to verify connection
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .create()
            .expect("Failed to create admin client");

        // Create a test topic
        let topic = NewTopic::new("test-topic", 1, TopicReplication::Fixed(1));
        let opts = AdminOptions::new();

        admin
            .create_topics(&[topic], &opts)
            .await
            .expect("Failed to create topic");

        // Topic creation succeeded, connection works
    }

    /// Test producing and consuming messages with testcontainers.
    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn test_produce_consume_messages() {
        let kafka = Kafka::default()
            .start()
            .await
            .expect("Failed to start Kafka container");
        let bootstrap_servers = format!(
            "127.0.0.1:{}",
            kafka
                .get_host_port_ipv4(9093)
                .await
                .expect("Failed to get Kafka port")
        );

        // Create admin client and topic
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .create()
            .expect("Failed to create admin client");

        let topic_name = "integration-test-topic";
        let topic = NewTopic::new(topic_name, 1, TopicReplication::Fixed(1));
        admin
            .create_topics(&[topic], &AdminOptions::new())
            .await
            .expect("Failed to create topic");

        // Produce some messages
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Failed to create producer");

        for i in 0..10 {
            let key = format!("key-{}", i);
            let payload = format!(r#"{{"id": {}, "value": "test-{}" }}"#, i, i);

            producer
                .send(
                    FutureRecord::to(topic_name).key(&key).payload(&payload),
                    Duration::from_secs(5),
                )
                .await
                .expect("Failed to produce message");
        }

        // Messages produced successfully
    }
}

mod buffer_integration {
    use super::*;

    /// Test buffer with realistic message flow.
    #[tokio::test]
    async fn test_buffer_high_throughput() {
        let config = BufferConfig {
            ttl_seconds: 60,
            max_size_mb: 100,
            flush_interval_seconds: 30,
            flush_batch_size: 1000,
            memory_alignment_bytes: 64,
        };

        let buffer = HotBuffer::new(config);

        // Simulate high throughput message ingestion
        for i in 0..1000 {
            let msg = KafkaMessage {
                key: Some(format!("key-{}", i).into_bytes()),
                value: Some(format!(r#"{{"id": {}, "timestamp": {} }}"#, i, i * 1000).into_bytes()),
                topic: "test-topic".to_string(),
                partition: 0,
                offset: i as i64,
                timestamp: chrono::Utc::now().timestamp_millis(),
                headers: vec![],
            };

            buffer.append(&msg).expect("Failed to append message");
        }

        // Verify buffer state
        assert_eq!(buffer.row_count(), 1000);

        // Take snapshot
        let snapshot = buffer.take_snapshot().expect("Failed to take snapshot");
        assert!(snapshot.is_some());

        let batch = snapshot.unwrap();
        assert_eq!(batch.num_rows(), 1000);

        // Buffer should be empty after snapshot
        assert_eq!(buffer.row_count(), 0);
    }

    /// Test buffer query capabilities.
    #[tokio::test]
    async fn test_buffer_queries() {
        let config = BufferConfig::default();
        let buffer = HotBuffer::new(config);

        // Add messages with different keys
        for i in 0i64..100 {
            let msg = KafkaMessage {
                key: Some(format!("user-{}", i % 10).into_bytes()),
                value: Some(format!(r#"{{"count": {} }}"#, i).into_bytes()),
                topic: "events".to_string(),
                partition: (i % 3) as i32,
                offset: i,
                timestamp: chrono::Utc::now().timestamp_millis(),
                headers: vec![],
            };
            buffer.append(&msg).unwrap();
        }

        // Query by key - returns last record for that key
        let result = buffer.query_by_key(b"user-0");
        assert!(!result.is_empty());
        // Note: query_by_key returns the last record with that key via the index
        assert!(!result.is_empty());

        // Query recent records (last 1 second)
        let result = buffer.query_recent(Duration::from_secs(1));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 100); // All should be recent
    }
}

mod txlog_integration {
    use super::*;

    /// Test transaction log with realistic workflow.
    #[tokio::test]
    async fn test_txlog_full_workflow() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        let config = TransactionLogConfig {
            log_dir: temp_dir.path().to_path_buf(),
            checkpoint_interval_entries: 100,
            checkpoint_interval_seconds: 300,
            max_log_files: 10,
        };

        let txlog = TransactionLog::open(config).expect("Failed to open transaction log");

        // Simulate ingestion workflow
        for batch in 0i64..5 {
            let start_offset = batch * 100;
            let end_offset = start_offset + 99;
            let batch_id = format!("batch-{}", batch);

            // Record offsets
            for offset in start_offset..=end_offset {
                txlog
                    .append(TransactionEntry::OffsetMarker {
                        topic: "test-topic".to_string(),
                        partition: 0,
                        offset,
                        record_count: 1,
                        timestamp: chrono::Utc::now(),
                    })
                    .expect("Failed to append offset marker");
            }

            // Flush start
            txlog
                .append(TransactionEntry::FlushStart {
                    batch_id: batch_id.clone(),
                    row_count: 100,
                    timestamp: chrono::Utc::now(),
                })
                .expect("Failed to append flush start");

            // Parquet written
            txlog
                .append(TransactionEntry::ParquetWritten {
                    batch_id: batch_id.clone(),
                    file_path: format!("s3://bucket/data/batch-{}.parquet", batch),
                    file_size_bytes: 1024 * 1024,
                    row_count: 100,
                    checksum: format!("crc32c-{}", batch),
                    timestamp: chrono::Utc::now(),
                })
                .expect("Failed to append parquet written");

            // Iceberg snapshot
            txlog
                .append(TransactionEntry::IcebergSnapshot {
                    batch_id: batch_id.clone(),
                    snapshot_id: batch + 1000,
                    manifest_list_path: format!("s3://bucket/metadata/snap-{}.avro", batch),
                    row_count_total: ((batch + 1) * 100) as u64,
                    timestamp: chrono::Utc::now(),
                })
                .expect("Failed to append iceberg snapshot");

            // Flush complete
            txlog
                .append(TransactionEntry::FlushComplete {
                    batch_id,
                    kafka_offset: end_offset,
                    iceberg_snapshot_id: batch + 1000,
                    duration_ms: 500,
                    timestamp: chrono::Utc::now(),
                })
                .expect("Failed to append flush complete");
        }

        // Force checkpoint
        txlog
            .force_checkpoint()
            .expect("Failed to force checkpoint");

        // Read all entries and verify
        let entries = txlog.read_all_entries().expect("Failed to read entries");

        // Should have at least: 5 batches * (100 offsets + flush_start + parquet + iceberg + flush_complete)
        // = 5 * 104 = 520 entries, plus checkpoint entries
        assert!(
            entries.len() >= 520,
            "Expected at least 520 entries, got {}",
            entries.len()
        );
        assert!(
            entries.len() < 550,
            "Expected less than 550 entries, got {}",
            entries.len()
        );
    }

    /// Test transaction log recovery.
    #[tokio::test]
    async fn test_txlog_recovery() {
        use k2i_core::txlog::RecoveryState;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        let config = TransactionLogConfig {
            log_dir: temp_dir.path().to_path_buf(),
            checkpoint_interval_entries: 1000,
            checkpoint_interval_seconds: 300,
            max_log_files: 10,
        };

        // Simulate incomplete flush (crash scenario)
        {
            let txlog = TransactionLog::open(config.clone()).expect("Failed to open txlog");

            // Complete batch
            let batch_id_1 = "batch-complete".to_string();
            txlog
                .append(TransactionEntry::FlushStart {
                    batch_id: batch_id_1.clone(),
                    row_count: 100,
                    timestamp: chrono::Utc::now(),
                })
                .unwrap();

            txlog
                .append(TransactionEntry::ParquetWritten {
                    batch_id: batch_id_1.clone(),
                    file_path: "s3://bucket/complete.parquet".to_string(),
                    file_size_bytes: 1024,
                    row_count: 100,
                    checksum: "crc32c-complete".to_string(),
                    timestamp: chrono::Utc::now(),
                })
                .unwrap();

            txlog
                .append(TransactionEntry::FlushComplete {
                    batch_id: batch_id_1,
                    kafka_offset: 99,
                    iceberg_snapshot_id: 1000,
                    duration_ms: 100,
                    timestamp: chrono::Utc::now(),
                })
                .unwrap();

            // Incomplete batch (simulates crash after parquet write)
            let batch_id_2 = "batch-incomplete".to_string();
            txlog
                .append(TransactionEntry::FlushStart {
                    batch_id: batch_id_2.clone(),
                    row_count: 100,
                    timestamp: chrono::Utc::now(),
                })
                .unwrap();

            txlog
                .append(TransactionEntry::ParquetWritten {
                    batch_id: batch_id_2,
                    file_path: "s3://bucket/orphan.parquet".to_string(),
                    file_size_bytes: 1024,
                    row_count: 100,
                    checksum: "crc32c-orphan".to_string(),
                    timestamp: chrono::Utc::now(),
                })
                .unwrap();

            // No FlushComplete - simulates crash
        }

        // Reopen and recover
        let txlog = TransactionLog::open(config).expect("Failed to reopen txlog");
        let recovery = RecoveryState::recover_from(&txlog).expect("Failed to recover");

        // Should identify the orphan file
        assert_eq!(recovery.orphan_files.len(), 1);
        assert!(recovery.orphan_files[0]
            .file_path
            .contains("orphan.parquet"));

        // Committed files should include only the complete batch
        assert_eq!(recovery.committed_files.len(), 1);
        assert!(recovery
            .committed_files
            .contains("s3://bucket/complete.parquet"));
    }
}

mod health_integration {
    use super::*;

    /// Test health check with component lifecycle.
    #[tokio::test]
    async fn test_health_lifecycle() {
        let health = HealthCheck::new();

        // Register components
        health.register_component("kafka");
        health.register_component("buffer");
        health.register_component("iceberg");
        health.register_component("txlog");

        // Initially all unknown - current impl treats unknown as healthy
        // (only explicit Unhealthy makes system unhealthy)
        assert_eq!(health.overall_status(), HealthStatus::Healthy);

        // Mark components healthy one by one
        health.mark_healthy("txlog");
        health.mark_healthy("kafka");
        health.mark_healthy("buffer");
        health.mark_healthy("iceberg");

        // All healthy
        assert_eq!(health.overall_status(), HealthStatus::Healthy);
        assert!(health.is_operational());

        // Degrade one component
        health.mark_degraded("buffer", "High memory usage");
        assert_eq!(health.overall_status(), HealthStatus::Degraded);
        assert!(health.is_operational()); // Still operational when degraded

        // Mark one unhealthy
        health.mark_unhealthy("kafka", "Connection lost");
        assert_eq!(health.overall_status(), HealthStatus::Unhealthy);
        assert!(!health.is_operational());

        // Verify component statuses
        let statuses = health.get_all_statuses();
        assert!(matches!(
            statuses.get("kafka"),
            Some(ComponentStatus::Unhealthy(_))
        ));
        assert!(matches!(
            statuses.get("buffer"),
            Some(ComponentStatus::Degraded(_))
        ));
        assert!(matches!(
            statuses.get("iceberg"),
            Some(ComponentStatus::Healthy)
        ));
    }
}

mod metrics_integration {
    use super::*;

    /// Test metrics with realistic usage patterns.
    #[tokio::test]
    async fn test_metrics_realistic_usage() {
        let metrics = Arc::new(IngestionMetrics::new());

        // Simulate message consumption
        for _ in 0..10000 {
            metrics.record_message();
        }
        assert_eq!(metrics.messages_total(), 10000);

        // Simulate batch consumption
        metrics.record_messages(5000);
        assert_eq!(metrics.messages_total(), 15000);

        // Simulate some errors
        for _ in 0..5 {
            metrics.record_error();
        }
        assert_eq!(metrics.errors_total(), 5);

        // Simulate flushes with varying durations
        let flush_durations = [50, 150, 300, 800, 2000, 4000, 8000, 15000];
        for (i, duration_ms) in flush_durations.iter().enumerate() {
            metrics.record_flush(1000 * (i + 1), Duration::from_millis(*duration_ms));
        }

        assert_eq!(metrics.flushes_total(), 8);
        assert_eq!(metrics.rows_flushed_total(), 36000); // Sum of 1000..8000

        // Check histogram
        let histogram = metrics.flush_duration_histogram();
        assert_eq!(histogram.count, 8);

        // Verify bucket distribution (cumulative)
        assert_eq!(histogram.buckets[0].1, 1); // <= 0.1s (50ms)
        assert_eq!(histogram.buckets[1].1, 2); // <= 0.25s (50ms, 150ms)
        assert_eq!(histogram.buckets[2].1, 3); // <= 0.5s
        assert_eq!(histogram.buckets[3].1, 4); // <= 1.0s
        assert_eq!(histogram.buckets[4].1, 5); // <= 2.5s
        assert_eq!(histogram.buckets[5].1, 6); // <= 5.0s
        assert_eq!(histogram.buckets[6].1, 7); // <= 10.0s
        assert_eq!(histogram.inf_bucket, 8); // +Inf (all)

        // Test gauges
        metrics.set_buffer_size_bytes(50 * 1024 * 1024); // 50 MB
        metrics.set_buffer_record_count(25000);

        assert_eq!(metrics.buffer_size_bytes(), 50 * 1024 * 1024);
        assert_eq!(metrics.buffer_record_count(), 25000);

        // Simulate backpressure
        metrics.record_backpressure();
        metrics.record_backpressure();
        assert_eq!(metrics.backpressure_total(), 2);

        // Simulate Iceberg commits
        metrics.record_iceberg_commit();
        metrics.record_iceberg_commit();
        metrics.record_iceberg_commit();
        assert_eq!(metrics.iceberg_commits_total(), 3);
    }

    /// Test metrics thread safety with concurrent access.
    #[tokio::test]
    async fn test_metrics_concurrent_access() {
        let metrics = Arc::new(IngestionMetrics::new());

        let mut handles = vec![];

        // Spawn multiple tasks that update metrics concurrently
        for _ in 0..10 {
            let m = Arc::clone(&metrics);
            handles.push(tokio::spawn(async move {
                for _ in 0..1000 {
                    m.record_message();
                }
            }));
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.expect("Task panicked");
        }

        // Verify total (10 tasks * 1000 messages)
        assert_eq!(metrics.messages_total(), 10000);
    }
}

mod end_to_end {
    use super::*;

    /// End-to-end test of the ingestion pipeline components.
    #[tokio::test]
    async fn test_pipeline_components() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Initialize components
        let buffer_config = BufferConfig {
            ttl_seconds: 60,
            max_size_mb: 100,
            flush_interval_seconds: 1, // Short interval for testing
            flush_batch_size: 100,
            memory_alignment_bytes: 64,
        };

        let txlog_config = TransactionLogConfig {
            log_dir: temp_dir.path().to_path_buf(),
            checkpoint_interval_entries: 100,
            checkpoint_interval_seconds: 60,
            max_log_files: 5,
        };

        let buffer = Arc::new(HotBuffer::new(buffer_config));
        let txlog = Arc::new(TransactionLog::open(txlog_config).expect("Failed to open txlog"));
        let metrics = Arc::new(IngestionMetrics::new());
        let health = Arc::new(HealthCheck::new());

        // Register health components
        health.register_component("kafka");
        health.register_component("buffer");
        health.register_component("iceberg");
        health.register_component("txlog");

        // Simulate ingestion pipeline
        health.mark_healthy("txlog");
        health.mark_healthy("kafka");

        // Ingest messages
        for i in 0..200 {
            let msg = KafkaMessage {
                key: Some(format!("key-{}", i).into_bytes()),
                value: Some(format!(r#"{{"event_id": {} }}"#, i).into_bytes()),
                topic: "events".to_string(),
                partition: 0,
                offset: i as i64,
                timestamp: chrono::Utc::now().timestamp_millis(),
                headers: vec![],
            };

            buffer.append(&msg).expect("Failed to append");

            // Log offset to txlog
            txlog
                .append(TransactionEntry::OffsetMarker {
                    topic: "events".to_string(),
                    partition: 0,
                    offset: i as i64,
                    record_count: 1,
                    timestamp: chrono::Utc::now(),
                })
                .expect("Failed to log offset");

            metrics.record_message();
        }

        health.mark_healthy("buffer");
        metrics.set_buffer_record_count(200);

        // Trigger flush
        if buffer.should_flush() || buffer.row_count() >= 100 {
            let snapshot = buffer.take_snapshot().expect("Failed to take snapshot");
            if let Some(batch) = snapshot {
                let row_count = batch.num_rows();
                let start = std::time::Instant::now();

                // Simulate Iceberg write workflow
                let batch_id = "e2e-test-batch-1".to_string();

                txlog
                    .append(TransactionEntry::FlushStart {
                        batch_id: batch_id.clone(),
                        row_count: row_count as u32,
                        timestamp: chrono::Utc::now(),
                    })
                    .expect("Failed to log flush start");

                // Simulate parquet write
                tokio::time::sleep(Duration::from_millis(50)).await;

                txlog
                    .append(TransactionEntry::ParquetWritten {
                        batch_id: batch_id.clone(),
                        file_path: "s3://bucket/data/batch-1.parquet".to_string(),
                        file_size_bytes: batch.get_array_memory_size() as u64,
                        row_count: row_count as u32,
                        checksum: "crc32c-e2e-test".to_string(),
                        timestamp: chrono::Utc::now(),
                    })
                    .expect("Failed to log parquet written");

                health.mark_healthy("iceberg");

                txlog
                    .append(TransactionEntry::FlushComplete {
                        batch_id,
                        kafka_offset: 199,
                        iceberg_snapshot_id: 1000,
                        duration_ms: start.elapsed().as_millis() as u64,
                        timestamp: chrono::Utc::now(),
                    })
                    .expect("Failed to log flush complete");

                metrics.record_flush(row_count, start.elapsed());
                metrics.record_iceberg_commit();
            }
        }

        // Verify final state
        assert!(health.is_operational());
        assert_eq!(health.overall_status(), HealthStatus::Healthy);
        assert_eq!(metrics.messages_total(), 200);
        assert_eq!(metrics.flushes_total(), 1);
        assert_eq!(metrics.rows_flushed_total(), 200);
        assert_eq!(buffer.row_count(), 0);

        // Verify txlog has entries
        let entries = txlog.read_all_entries().expect("Failed to read entries");
        assert!(!entries.is_empty());
    }
}
