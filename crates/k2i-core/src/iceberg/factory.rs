//! Catalog factory for creating catalog instances.
//!
//! This module provides a factory pattern for creating different Iceberg catalog
//! implementations based on configuration. Supported catalog types:
//!
//! - REST Catalog (via `iceberg-catalog-rest`)
//! - AWS Glue Catalog (planned)
//! - Hive Metastore (planned)
//! - Nessie (planned)

use crate::config::{CatalogType, IcebergConfig};
use crate::{Error, IcebergError, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Trait for creating catalog instances.
///
/// Implementations of this trait are responsible for creating and configuring
/// catalog connections based on the provided configuration.
#[async_trait]
pub trait CatalogFactory: Send + Sync {
    /// Create a new catalog instance.
    ///
    /// # Arguments
    /// * `config` - The Iceberg configuration containing catalog settings
    ///
    /// # Returns
    /// A boxed catalog instance or an error if creation fails.
    async fn create(&self, config: &IcebergConfig) -> Result<Arc<dyn CatalogOperations>>;

    /// Get the catalog type this factory creates.
    fn catalog_type(&self) -> CatalogType;

    /// Check if this factory supports the given configuration.
    fn supports(&self, config: &IcebergConfig) -> bool {
        self.catalog_type() == config.catalog_type
    }
}

/// Trait defining catalog operations.
///
/// This trait abstracts the operations that can be performed on any Iceberg catalog,
/// regardless of the underlying implementation (REST, Glue, Hive, etc.).
#[async_trait]
pub trait CatalogOperations: Send + Sync {
    /// Check if the catalog connection is healthy.
    async fn health_check(&self) -> Result<CatalogHealth>;

    /// List all namespaces in the catalog.
    async fn list_namespaces(&self) -> Result<Vec<String>>;

    /// Check if a namespace exists.
    async fn namespace_exists(&self, namespace: &str) -> Result<bool>;

    /// Create a namespace if it doesn't exist.
    async fn create_namespace(&self, namespace: &str) -> Result<()>;

    /// List all tables in a namespace.
    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>>;

    /// Check if a table exists.
    async fn table_exists(&self, namespace: &str, table: &str) -> Result<bool>;

    /// Load table metadata.
    async fn load_table(&self, namespace: &str, table: &str) -> Result<TableInfo>;

    /// Create a new table.
    async fn create_table(
        &self,
        namespace: &str,
        table: &str,
        schema: &TableSchema,
    ) -> Result<TableInfo>;

    /// Get the current snapshot ID for a table.
    async fn current_snapshot_id(&self, namespace: &str, table: &str) -> Result<Option<i64>>;

    /// Commit a new snapshot to the table.
    ///
    /// This is an atomic operation that uses compare-and-swap semantics.
    async fn commit_snapshot(
        &self,
        namespace: &str,
        table: &str,
        commit: SnapshotCommit,
    ) -> Result<SnapshotCommitResult>;

    /// Return the real manifest-list path recorded for a committed snapshot,
    /// when the catalog implementation exposes it.
    ///
    /// The default keeps existing catalog implementations source-compatible.
    /// Callers must tolerate `None` because not every backend produces
    /// official Iceberg metadata through this abstraction yet.
    fn manifest_list_path_for_snapshot(&self, _snapshot_id: i64) -> Option<String> {
        None
    }

    /// Commit a new current table schema.
    ///
    /// Catalogs that do not implement Iceberg table-update commits return an
    /// explicit schema-evolution error so ingestion can pause before advancing
    /// Kafka offsets.
    async fn update_schema(
        &self,
        namespace: &str,
        table: &str,
        schema: &TableSchema,
        expected_schema_id: Option<i32>,
    ) -> Result<TableInfo> {
        let _ = (namespace, table, schema, expected_schema_id);
        Err(Error::Iceberg(IcebergError::SchemaEvolution(format!(
            "{:?} catalog does not support schema update commits",
            self.catalog_type()
        ))))
    }

    /// Get the catalog type.
    fn catalog_type(&self) -> CatalogType;

    /// Get the warehouse path.
    fn warehouse_path(&self) -> &str;

    /// Close the catalog connection gracefully.
    async fn close(&self) -> Result<()>;
}

/// Health status of a catalog connection.
#[derive(Debug, Clone)]
pub struct CatalogHealth {
    /// Whether the catalog is healthy
    pub is_healthy: bool,
    /// Response time in milliseconds
    pub response_time_ms: u64,
    /// Optional message with details
    pub message: Option<String>,
    /// Catalog type
    pub catalog_type: CatalogType,
}

/// Information about an Iceberg table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    /// Namespace (database)
    pub namespace: String,
    /// Table name
    pub name: String,
    /// Table location in storage
    pub location: String,
    /// Current snapshot ID (if any)
    pub current_snapshot_id: Option<i64>,
    /// Table schema
    pub schema: TableSchema,
    /// Table properties
    pub properties: std::collections::HashMap<String, String>,
}

/// Iceberg table schema representation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableSchema {
    /// Schema ID
    pub schema_id: i32,
    /// Schema fields
    pub fields: Vec<SchemaFieldInfo>,
}

/// Schema field information.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaFieldInfo {
    /// Field ID
    pub id: i32,
    /// Field name
    pub name: String,
    /// Field type (e.g., "string", "long", "timestamp")
    pub field_type: String,
    /// Whether the field is required
    pub required: bool,
    /// Optional documentation
    pub doc: Option<String>,
}

/// A snapshot commit request.
#[derive(Debug, Clone)]
pub struct SnapshotCommit {
    /// Expected current snapshot ID (for CAS)
    pub expected_snapshot_id: Option<i64>,
    /// Files to add in this snapshot
    pub files_to_add: Vec<DataFileInfo>,
    /// Files to remove in this snapshot (for compaction)
    pub files_to_remove: Vec<String>,
    /// Snapshot summary properties
    pub summary: std::collections::HashMap<String, String>,
}

/// Information about a data file to add.
#[derive(Debug, Clone)]
pub struct DataFileInfo {
    /// File path in storage
    pub file_path: String,
    /// File size in bytes
    pub file_size_bytes: u64,
    /// Number of records in the file
    pub record_count: u64,
    /// Partition values (if partitioned)
    pub partition_values: std::collections::HashMap<String, String>,
    /// File format (typically "parquet")
    pub file_format: String,
}

/// Result of a snapshot commit.
#[derive(Debug, Clone)]
pub struct SnapshotCommitResult {
    /// The new snapshot ID
    pub snapshot_id: i64,
    /// Timestamp when the snapshot was created
    pub committed_at: chrono::DateTime<chrono::Utc>,
    /// Number of files added
    pub files_added: usize,
    /// Number of files removed
    pub files_removed: usize,
}

/// Registry of catalog factories.
///
/// Manages available catalog factory implementations and provides
/// a way to create catalogs based on configuration.
pub struct CatalogFactoryRegistry {
    factories: Vec<Box<dyn CatalogFactory>>,
}

impl CatalogFactoryRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            factories: Vec::new(),
        }
    }

    /// Create a registry with default factories.
    pub fn with_defaults() -> Self {
        let mut registry = Self::new();
        // Register default factories
        registry.register(Box::new(RestCatalogFactory));
        registry.register(Box::new(super::nessie::NessieCatalogFactory));
        registry.register(Box::new(super::glue::GlueCatalogFactory));
        registry.register(Box::new(super::hive::HiveCatalogFactory));
        registry.register(Box::new(super::sql::SqlCatalogFactory));
        registry
    }

    /// Register a new catalog factory.
    pub fn register(&mut self, factory: Box<dyn CatalogFactory>) {
        self.factories.push(factory);
    }

    /// Create a catalog instance based on configuration.
    pub async fn create(&self, config: &IcebergConfig) -> Result<Arc<dyn CatalogOperations>> {
        for factory in &self.factories {
            if factory.supports(config) {
                return factory.create(config).await;
            }
        }

        Err(Error::Config(format!(
            "No catalog factory found for catalog type: {:?}",
            config.catalog_type
        )))
    }

    /// Get the supported catalog types.
    pub fn supported_types(&self) -> Vec<CatalogType> {
        self.factories.iter().map(|f| f.catalog_type()).collect()
    }
}

impl Default for CatalogFactoryRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Factory for REST catalog.
pub struct RestCatalogFactory;

#[async_trait]
impl CatalogFactory for RestCatalogFactory {
    async fn create(&self, config: &IcebergConfig) -> Result<Arc<dyn CatalogOperations>> {
        let committer = crate::iceberg::official::OfficialRestCommitter::new(config).await?;
        Ok(Arc::new(committer))
    }

    fn catalog_type(&self) -> CatalogType {
        CatalogType::Rest
    }
}

/// Backward-compatible adapter for the former hand-written REST client.
///
/// New code should construct [`crate::iceberg::OfficialRestCommitter`]
/// directly or use [`RestCatalogFactory`]. This adapter preserves the public
/// constructor and trait implementation released in K2I 0.2.x while routing
/// every operation through the official Apache Iceberg REST client.
pub struct RestCatalogClient {
    inner: super::official::OfficialRestCommitter,
}

impl RestCatalogClient {
    /// Create a REST catalog client using the supplied endpoint.
    pub async fn new(config: &IcebergConfig, rest_uri: String) -> Result<Self> {
        let mut config = config.clone();
        config.rest_uri = Some(rest_uri);
        Ok(Self {
            inner: super::official::OfficialRestCommitter::new(&config).await?,
        })
    }
}

#[async_trait]
impl CatalogOperations for RestCatalogClient {
    async fn health_check(&self) -> Result<CatalogHealth> {
        self.inner.health_check().await
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        self.inner.list_namespaces().await
    }

    async fn namespace_exists(&self, namespace: &str) -> Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn create_namespace(&self, namespace: &str) -> Result<()> {
        self.inner.create_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        self.inner.list_tables(namespace).await
    }

    async fn table_exists(&self, namespace: &str, table: &str) -> Result<bool> {
        self.inner.table_exists(namespace, table).await
    }

    async fn load_table(&self, namespace: &str, table: &str) -> Result<TableInfo> {
        self.inner.load_table(namespace, table).await
    }

    async fn create_table(
        &self,
        namespace: &str,
        table: &str,
        schema: &TableSchema,
    ) -> Result<TableInfo> {
        self.inner.create_table(namespace, table, schema).await
    }

    async fn current_snapshot_id(&self, namespace: &str, table: &str) -> Result<Option<i64>> {
        self.inner.current_snapshot_id(namespace, table).await
    }

    async fn commit_snapshot(
        &self,
        namespace: &str,
        table: &str,
        commit: SnapshotCommit,
    ) -> Result<SnapshotCommitResult> {
        self.inner.commit_snapshot(namespace, table, commit).await
    }

    fn manifest_list_path_for_snapshot(&self, snapshot_id: i64) -> Option<String> {
        self.inner.manifest_list_path_for_snapshot(snapshot_id)
    }

    async fn update_schema(
        &self,
        namespace: &str,
        table: &str,
        schema: &TableSchema,
        expected_schema_id: Option<i32>,
    ) -> Result<TableInfo> {
        self.inner
            .update_schema(namespace, table, schema, expected_schema_id)
            .await
    }

    fn catalog_type(&self) -> CatalogType {
        self.inner.catalog_type()
    }

    fn warehouse_path(&self) -> &str {
        self.inner.warehouse_path()
    }

    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_factory_registry_creation() {
        let registry = CatalogFactoryRegistry::new();
        assert!(registry.supported_types().is_empty());
    }

    #[test]
    fn test_catalog_factory_registry_with_defaults() {
        let registry = CatalogFactoryRegistry::with_defaults();
        let types = registry.supported_types();
        assert!(types.contains(&CatalogType::Rest));
    }
}
