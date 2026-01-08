# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial release of K2I (Kafka to Iceberg) streaming ingestion engine
- Kafka consumer with smart backpressure and retry logic
- Hot buffer with Apache Arrow for sub-second query freshness
- Iceberg writer with support for REST, Hive, Glue, and Nessie catalogs
- Transaction log for crash recovery and exactly-once semantics
- Automatic maintenance tasks (compaction, snapshot expiration, orphan cleanup)
- Prometheus metrics and health check endpoints
- CLI commands: `ingest`, `status`, `maintenance`, `validate`
- Comprehensive documentation

### Features
- **Kafka Consumer**: rdkafka-based consumer with cooperative sticky assignment
- **Hot Buffer**: Arrow RecordBatch with DashMap indexes for O(1) lookups
- **Iceberg Writer**: Parquet files with Snappy/Zstd/LZ4 compression
- **Transaction Log**: Append-only log with CRC32 checksums
- **Health Checks**: Component-level health tracking with Kubernetes-compatible endpoints
- **Metrics**: Prometheus counters, gauges, and histograms
- **Circuit Breaker**: Fault tolerance for catalog operations

## [0.1.0] - TBD

Initial release.
