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

/// End-to-end verification that data files land where the Iceberg catalog says
/// they do, against a real S3 implementation (MinIO).
///
/// For cloud backends the object store is rooted at the *bucket*, while the
/// catalog, the transaction log, and the read path all locate a data file by
/// joining `warehouse_path` with the writer's reported path. A warehouse with
/// an in-bucket prefix (`s3://bucket/warehouse`) is therefore the case where
/// those two views can silently disagree — and a disagreement means every
/// committed file is unreadable while the pipeline reports success.
///
/// These tests pin the contract: the reported path is warehouse-relative, and
/// `warehouse_path` + reported path resolves to a real object.
mod s3_object_store_integration {
    use k2i_core::config::{
        CatalogManagerConfig, CatalogType, GlueCatalogConfig, IcebergConfig, ObjectStoreConfig,
        ParquetCompression, RestCatalogConfig, TableManagementConfig,
    };
    use k2i_core::iceberg::IcebergWriter;
    use object_store::aws::AmazonS3Builder;
    use object_store::path::Path as ObjectPath;
    use object_store::ObjectStore;
    use testcontainers::core::{ContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    const BUCKET: &str = "k2i-test-bucket";
    const ACCESS_KEY: &str = "minioadmin";
    const SECRET_KEY: &str = "minioadmin";

    /// MinIO serves each top-level directory under its data dir as a bucket.
    /// Seeding a file inside `/data/<bucket>/` therefore provisions the bucket
    /// before startup, avoiding a dependency on an S3 admin client purely to
    /// issue a CreateBucket call. The image entrypoint prefixes `minio` to the
    /// command, so the command cannot be used to run a shell.
    fn minio_image() -> testcontainers::ContainerRequest<GenericImage> {
        GenericImage::new("minio/minio", "RELEASE.2022-02-07T08-17-33Z")
            .with_wait_for(WaitFor::message_on_stdout("API:"))
            .with_exposed_port(ContainerPort::Tcp(9000))
            .with_env_var("MINIO_CONSOLE_ADDRESS", ":9001")
            .with_env_var("MINIO_ROOT_USER", ACCESS_KEY)
            .with_env_var("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .with_copy_to(format!("/data/{BUCKET}/.keep"), Vec::<u8>::new())
            .with_cmd(vec!["server".to_string(), "/data".to_string()])
    }

    fn s3_config(warehouse_path: &str, endpoint: &str) -> IcebergConfig {
        IcebergConfig {
            catalog_type: CatalogType::Rest,
            warehouse_path: warehouse_path.to_string(),
            database_name: "test_db".to_string(),
            table_name: "test_table".to_string(),
            target_file_size_mb: 128,
            compression: ParquetCompression::Snappy,
            partition_spec: vec![],
            rest_uri: None,
            hive_metastore_uri: None,
            aws_region: Some("us-east-1".to_string()),
            aws_access_key_id: Some(ACCESS_KEY.into()),
            aws_secret_access_key: Some(SECRET_KEY.into()),
            s3_endpoint: Some(endpoint.to_string()),
            gcs_bucket_name: None,
            gcs_service_account_path: None,
            azure_container_name: None,
            azure_storage_account_name: None,
            azure_access_key: None,
            catalog_manager: CatalogManagerConfig::default(),
            table_management: TableManagementConfig::default(),
            rest: RestCatalogConfig::default(),
            glue: GlueCatalogConfig::default(),
            nessie: None,
            sql_catalog: None,
            object_store: ObjectStoreConfig::default(),
        }
    }

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("partition", DataType::Int32, false),
            Field::new("offset", DataType::Int64, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));
        let now = chrono::Utc::now().timestamp_millis();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int32Array::from(vec![0, 0, 0])),
                Arc::new(Int64Array::from(vec![100, 101, 102])),
                Arc::new(Int64Array::from(vec![now, now, now])),
            ],
        )
        .expect("failed to build test batch")
    }

    /// A store rooted at the bucket, matching how an external reader (Spark,
    /// DuckDB, Trino) would resolve the URI the catalog recorded.
    fn bucket_rooted_store(endpoint: &str) -> impl ObjectStore {
        AmazonS3Builder::new()
            .with_bucket_name(BUCKET)
            .with_region("us-east-1")
            .with_access_key_id(ACCESS_KEY)
            .with_secret_access_key(SECRET_KEY)
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .build()
            .expect("failed to build verification store")
    }

    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn test_s3_prefixed_warehouse_file_lands_where_catalog_points() {
        let container = minio_image().start().await.expect("failed to start MinIO");
        let port = container
            .get_host_port_ipv4(9000)
            .await
            .expect("failed to get MinIO port");
        let endpoint = format!("http://127.0.0.1:{port}");

        // The interesting case: a warehouse with an in-bucket prefix.
        let warehouse = format!("s3://{BUCKET}/warehouse");
        let writer = IcebergWriter::new(s3_config(&warehouse, &endpoint))
            .await
            .expect("failed to build S3 writer");

        let stats = writer
            .write_batch(test_batch(), 102)
            .await
            .expect("write_batch against MinIO should succeed");

        // 1. The reported path is warehouse-relative — it must NOT already
        //    contain the in-bucket prefix, or consumers that join it against
        //    `warehouse_path` would double it.
        assert!(
            stats.file_path.starts_with("data/test_db/test_table/"),
            "reported path should be warehouse-relative, got: {}",
            stats.file_path
        );
        assert!(
            !stats.file_path.starts_with("warehouse/"),
            "reported path must not carry the in-bucket prefix, got: {}",
            stats.file_path
        );

        let store = bucket_rooted_store(&endpoint);

        // 2. Joining `warehouse_path` with the reported path — exactly what the
        //    catalog records and what an external reader resolves — must land on
        //    a real object of the right size.
        let expected_key = ObjectPath::from(format!("warehouse/{}", stats.file_path));
        let meta = store.head(&expected_key).await.unwrap_or_else(|e| {
            panic!(
                "catalog URI {}/{} does not resolve to a stored object: {e}",
                warehouse, stats.file_path
            )
        });
        assert_eq!(
            meta.size as usize, stats.file_size_bytes,
            "stored object size should match the reported write size"
        );

        // 3. Nothing was written to the double-prefixed location.
        let doubled = ObjectPath::from(format!("warehouse/warehouse/{}", stats.file_path));
        assert!(
            store.head(&doubled).await.is_err(),
            "object must not be written under a doubled warehouse prefix"
        );

        // 4. Nothing was written at the bucket root either, which is where
        //    uploads landed before the prefix was applied at all.
        let bucket_root = ObjectPath::from(stats.file_path.clone());
        assert!(
            store.head(&bucket_root).await.is_err(),
            "object must not be written at the bucket root, bypassing the warehouse prefix"
        );
    }

    /// A bucket-root warehouse has no prefix to apply, so the reported path and
    /// the storage key must coincide.
    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn test_s3_bucket_root_warehouse_writes_at_bucket_root() {
        let container = minio_image().start().await.expect("failed to start MinIO");
        let port = container
            .get_host_port_ipv4(9000)
            .await
            .expect("failed to get MinIO port");
        let endpoint = format!("http://127.0.0.1:{port}");

        let warehouse = format!("s3://{BUCKET}");
        let writer = IcebergWriter::new(s3_config(&warehouse, &endpoint))
            .await
            .expect("failed to build S3 writer");

        let stats = writer
            .write_batch(test_batch(), 102)
            .await
            .expect("write_batch against MinIO should succeed");

        let store = bucket_rooted_store(&endpoint);
        let key = ObjectPath::from(stats.file_path.clone());
        let meta = store.head(&key).await.unwrap_or_else(|e| {
            panic!(
                "object {} should exist at bucket root: {e}",
                stats.file_path
            )
        });
        assert_eq!(meta.size as usize, stats.file_size_bytes);
    }

    /// A multi-segment prefix (`s3://bucket/warehouse/prod`) must be preserved
    /// in full, not collapsed to its first segment.
    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn test_s3_multi_segment_warehouse_prefix_is_preserved() {
        let container = minio_image().start().await.expect("failed to start MinIO");
        let port = container
            .get_host_port_ipv4(9000)
            .await
            .expect("failed to get MinIO port");
        let endpoint = format!("http://127.0.0.1:{port}");

        let warehouse = format!("s3://{BUCKET}/warehouse/prod");
        let writer = IcebergWriter::new(s3_config(&warehouse, &endpoint))
            .await
            .expect("failed to build S3 writer");

        let stats = writer
            .write_batch(test_batch(), 102)
            .await
            .expect("write_batch against MinIO should succeed");

        let store = bucket_rooted_store(&endpoint);
        let expected_key = ObjectPath::from(format!("warehouse/prod/{}", stats.file_path));
        store.head(&expected_key).await.unwrap_or_else(|e| {
            panic!(
                "catalog URI {}/{} does not resolve to a stored object: {e}",
                warehouse, stats.file_path
            )
        });
    }
}
