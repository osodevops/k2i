//! Iceberg writer with atomic commits.
//!
//! This module provides functionality for writing data to Apache Iceberg tables:
//!
//! - Arrow → Parquet conversion with configurable compression
//! - Object storage upload (S3, GCS, Azure, local filesystem)
//! - Iceberg catalog integration (REST, Hive, Glue, Nessie)
//! - Atomic commits with transaction log integration
//! - Catalog factory pattern for multi-catalog support
//! - Metadata caching with TTL-based invalidation
//! - Table lifecycle management (create, load, schema evolution)
//! - Transaction coordination with CAS and idempotency

mod catalog;
mod factory;
mod glue;
mod hive;
mod metadata_cache;
mod nessie;
pub mod rest_api;
pub mod schema_evolution;
mod table_manager;
mod transaction_coordinator;
mod writer;

pub use catalog::{CatalogManager, TableMetadata};
pub use factory::{
    CatalogFactory, CatalogFactoryRegistry, CatalogHealth, CatalogOperations, DataFileInfo,
    RestCatalogClient, RestCatalogFactory, SchemaFieldInfo, SnapshotCommit, SnapshotCommitResult,
    TableInfo, TableSchema,
};
pub use glue::{GlueCatalogClient, GlueCatalogFactory, GlueClientConfig};
pub use hive::{HiveCatalogClient, HiveCatalogFactory, HiveClientConfig};
pub use metadata_cache::{
    create_shared_cache, create_shared_cache_with_config, ttl, CacheStats, CacheSummary,
    CachedField, CachedManifestEntry, CachedPartitionField, CachedPartitionSpec, CachedSchema,
    CachedValue, MetadataCache, MetadataCacheConfig, SharedMetadataCache,
};
pub use nessie::{NessieCatalogClient, NessieCatalogFactory, NessieClientConfig};
pub use schema_evolution::{
    IcebergType, InferredField, SchemaDiff, SchemaEvolution, SchemaEvolutionConfig,
    SchemaEvolutionPlan, SchemaEvolver, SchemaInference, TypeChange,
};
pub use table_manager::{SchemaMismatch, TableManager, TableManagerBuilder};
pub use transaction_coordinator::{
    CommitResult, IdempotencyKey, IdempotencyRecord, TransactionCoordinator,
    TransactionCoordinatorBuilder, TransactionCoordinatorConfig, TransactionStats,
};
pub use writer::{IcebergWriter, IcebergWriterBuilder, PartitionInfo, WriteStats};
