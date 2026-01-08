//! Table manager for Iceberg table lifecycle.
//!
//! Handles table operations:
//! - Create table if not exists
//! - Load table metadata
//! - Schema validation and evolution
//! - Partition spec management

use crate::config::{IcebergConfig, SchemaMismatchAction, TableManagementConfig};
use crate::iceberg::factory::{CatalogOperations, SchemaFieldInfo, TableInfo, TableSchema};
use crate::iceberg::metadata_cache::{CachedField, CachedSchema, MetadataCache};
use crate::iceberg::schema_evolution::{
    IcebergType, InferredField, SchemaEvolution, SchemaEvolutionConfig, SchemaEvolutionPlan,
    SchemaEvolver,
};
use crate::{Error, Result};
use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Table manager for handling table lifecycle operations.
pub struct TableManager {
    /// Catalog operations
    catalog: Arc<dyn CatalogOperations>,
    /// Namespace (database)
    namespace: String,
    /// Table name
    table_name: String,
    /// Table management configuration
    config: TableManagementConfig,
    /// Metadata cache
    cache: Arc<MetadataCache>,
    /// Full table identifier
    full_table_name: String,
}

impl TableManager {
    /// Create a new table manager.
    pub fn new(
        catalog: Arc<dyn CatalogOperations>,
        namespace: String,
        table_name: String,
        config: TableManagementConfig,
        cache: Arc<MetadataCache>,
    ) -> Self {
        let full_table_name = format!("{}.{}", namespace, table_name);
        Self {
            catalog,
            namespace,
            table_name,
            config,
            cache,
            full_table_name,
        }
    }

    /// Create a table manager from IcebergConfig.
    pub fn from_config(
        catalog: Arc<dyn CatalogOperations>,
        config: &IcebergConfig,
        cache: Arc<MetadataCache>,
    ) -> Self {
        Self::new(
            catalog,
            config.database_name.clone(),
            config.table_name.clone(),
            config.table_management.clone(),
            cache,
        )
    }

    /// Get or create the table.
    ///
    /// If the table exists, loads and validates it.
    /// If it doesn't exist and auto_create is enabled, creates it.
    pub async fn get_or_create(&self, expected_schema: &TableSchema) -> Result<TableInfo> {
        // Check if table exists
        let exists = self
            .catalog
            .table_exists(&self.namespace, &self.table_name)
            .await?;

        if exists {
            // Load existing table
            let table_info = self.load_table().await?;

            // Validate schema compatibility
            self.validate_schema(&table_info.schema, expected_schema)?;

            Ok(table_info)
        } else if self.config.auto_create_if_missing {
            // Create new table
            info!(
                table = %self.full_table_name,
                "Table does not exist, creating with auto_create_if_missing=true"
            );
            self.create_table(expected_schema).await
        } else {
            Err(Error::Iceberg(crate::IcebergError::TableNotFound(
                self.full_table_name.clone(),
            )))
        }
    }

    /// Load table metadata.
    pub async fn load_table(&self) -> Result<TableInfo> {
        let table_info = self
            .catalog
            .load_table(&self.namespace, &self.table_name)
            .await?;

        // Update cache
        self.update_cache(&table_info);

        debug!(
            table = %self.full_table_name,
            snapshot_id = ?table_info.current_snapshot_id,
            "Loaded table metadata"
        );

        Ok(table_info)
    }

    /// Create a new table.
    pub async fn create_table(&self, schema: &TableSchema) -> Result<TableInfo> {
        // Ensure namespace exists
        let ns_exists = self.catalog.namespace_exists(&self.namespace).await?;
        if !ns_exists {
            info!(namespace = %self.namespace, "Creating namespace");
            self.catalog.create_namespace(&self.namespace).await?;
        }

        // Create table
        let table_info = self
            .catalog
            .create_table(&self.namespace, &self.table_name, schema)
            .await?;

        info!(
            table = %self.full_table_name,
            location = %table_info.location,
            "Created new table"
        );

        // Update cache
        self.update_cache(&table_info);

        Ok(table_info)
    }

    /// Validate schema compatibility between existing and expected schema.
    fn validate_schema(&self, existing: &TableSchema, expected: &TableSchema) -> Result<()> {
        let mismatches = self.find_schema_mismatches(existing, expected);

        if mismatches.is_empty() {
            return Ok(());
        }

        match self.config.schema_mismatch_action {
            SchemaMismatchAction::Fail => {
                Err(Error::Iceberg(crate::IcebergError::SchemaMismatch {
                    expected: format!("{:?}", expected),
                    actual: format!("{:?}", existing),
                }))
            }
            SchemaMismatchAction::Warn => {
                warn!(
                    table = %self.full_table_name,
                    mismatches = ?mismatches,
                    "Schema mismatch detected, continuing with warning"
                );
                Ok(())
            }
            SchemaMismatchAction::Evolve => {
                info!(
                    table = %self.full_table_name,
                    mismatches = ?mismatches,
                    "Schema mismatch detected, attempting schema evolution"
                );

                // Convert expected schema fields to InferredFields for evolution comparison
                let inferred_fields: Vec<InferredField> = expected
                    .fields
                    .iter()
                    .map(|f| InferredField {
                        name: f.name.clone(),
                        field_type: IcebergType::from_iceberg_string(&f.field_type),
                        required: f.required,
                        seen_count: 1,
                        total_records: 1,
                    })
                    .collect();

                // Compare schemas
                let diff = SchemaEvolution::compare(existing, &inferred_fields);

                // Validate evolution is possible
                SchemaEvolution::validate_evolution(&diff)?;

                // Calculate next field ID
                let max_existing_id = existing.fields.iter().map(|f| f.id).max().unwrap_or(0);
                let next_field_id = max_existing_id + 1;

                // Get fields to add
                let fields_to_add = SchemaEvolution::fields_to_add(&diff, next_field_id);

                if !fields_to_add.is_empty() {
                    info!(
                        table = %self.full_table_name,
                        fields_added = fields_to_add.len(),
                        field_names = ?fields_to_add.iter().map(|f| &f.name).collect::<Vec<_>>(),
                        "Schema evolution: adding new fields"
                    );
                }

                // Log safe type promotions
                for change in diff.type_changes.iter().filter(|c| c.is_safe_promotion) {
                    info!(
                        table = %self.full_table_name,
                        field = %change.name,
                        from = %change.existing_type,
                        to = %change.incoming_type.to_iceberg_string(),
                        "Schema evolution: type promotion detected (will be handled during write)"
                    );
                }

                // Note: Actual schema update to catalog would happen here
                // For now, we validate and log - the caller should use the evolved schema
                // In a full implementation, we would call catalog.update_schema()

                Ok(())
            }
        }
    }

    /// Find mismatches between two schemas.
    fn find_schema_mismatches(
        &self,
        existing: &TableSchema,
        expected: &TableSchema,
    ) -> Vec<SchemaMismatch> {
        let mut mismatches = Vec::new();

        // Build lookup for existing fields
        let existing_fields: std::collections::HashMap<&str, &SchemaFieldInfo> = existing
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f))
            .collect();

        // Check expected fields
        for expected_field in &expected.fields {
            match existing_fields.get(expected_field.name.as_str()) {
                None => {
                    mismatches.push(SchemaMismatch::MissingField {
                        name: expected_field.name.clone(),
                    });
                }
                Some(existing_field) => {
                    if existing_field.field_type != expected_field.field_type {
                        mismatches.push(SchemaMismatch::TypeMismatch {
                            name: expected_field.name.clone(),
                            expected: expected_field.field_type.clone(),
                            actual: existing_field.field_type.clone(),
                        });
                    }
                    if existing_field.required != expected_field.required {
                        mismatches.push(SchemaMismatch::NullabilityMismatch {
                            name: expected_field.name.clone(),
                            expected_required: expected_field.required,
                            actual_required: existing_field.required,
                        });
                    }
                }
            }
        }

        // Check for extra fields in existing schema (informational only)
        let expected_fields: std::collections::HashSet<&str> =
            expected.fields.iter().map(|f| f.name.as_str()).collect();

        for existing_field in &existing.fields {
            if !expected_fields.contains(existing_field.name.as_str()) {
                mismatches.push(SchemaMismatch::ExtraField {
                    name: existing_field.name.clone(),
                });
            }
        }

        mismatches
    }

    /// Update the metadata cache with table info.
    fn update_cache(&self, table_info: &TableInfo) {
        // Cache snapshot ID
        if let Some(snapshot_id) = table_info.current_snapshot_id {
            self.cache.set_snapshot_id(snapshot_id);
        }

        // Cache schema
        let cached_schema = CachedSchema {
            schema_id: table_info.schema.schema_id,
            fields: table_info
                .schema
                .fields
                .iter()
                .map(|f| CachedField {
                    id: f.id,
                    name: f.name.clone(),
                    field_type: f.field_type.clone(),
                    required: f.required,
                })
                .collect(),
        };
        self.cache.set_schema(cached_schema);

        // Cache table location
        self.cache.set_table_location(table_info.location.clone());
    }

    /// Get the current snapshot ID from cache or catalog.
    pub async fn current_snapshot_id(&self) -> Result<Option<i64>> {
        // Try cache first
        if let Some(snapshot_id) = self.cache.get_snapshot_id() {
            return Ok(Some(snapshot_id));
        }

        // Fall back to catalog
        let snapshot_id = self
            .catalog
            .current_snapshot_id(&self.namespace, &self.table_name)
            .await?;

        // Update cache if we got a value
        if let Some(id) = snapshot_id {
            self.cache.set_snapshot_id(id);
        }

        Ok(snapshot_id)
    }

    /// Get the schema from cache or catalog.
    pub async fn get_schema(&self) -> Result<CachedSchema> {
        // Try cache first
        if let Some(schema) = self.cache.get_schema() {
            return Ok(schema);
        }

        // Fall back to catalog
        let _table_info = self.load_table().await?;

        // Cache is already updated by load_table
        self.cache.get_schema().ok_or_else(|| {
            Error::Iceberg(crate::IcebergError::Other(
                "Failed to cache schema after load".to_string(),
            ))
        })
    }

    /// Get the table location from cache or catalog.
    pub async fn get_location(&self) -> Result<String> {
        // Try cache first
        if let Some(location) = self.cache.get_table_location() {
            return Ok(location);
        }

        // Fall back to catalog
        let table_info = self.load_table().await?;
        Ok(table_info.location)
    }

    /// Refresh all cached metadata.
    pub async fn refresh_metadata(&self) -> Result<()> {
        self.cache.invalidate_all();
        self.load_table().await?;
        Ok(())
    }

    /// Get the full table name.
    pub fn full_table_name(&self) -> &str {
        &self.full_table_name
    }

    /// Get the namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Get the table name.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Check if the table exists.
    pub async fn exists(&self) -> Result<bool> {
        self.catalog
            .table_exists(&self.namespace, &self.table_name)
            .await
    }

    /// Detect and plan schema evolution from incoming JSON records.
    ///
    /// Returns a schema evolution plan if changes are needed, or None if the schema is compatible.
    pub fn detect_schema_evolution(
        &self,
        records: &[Value],
        evolution_config: Option<SchemaEvolutionConfig>,
    ) -> Result<Option<SchemaEvolutionPlan>> {
        // Get current schema from cache or fail
        let cached_schema = self.cache.get_schema().ok_or_else(|| {
            Error::Iceberg(crate::IcebergError::Other(
                "No schema in cache - table not loaded".to_string(),
            ))
        })?;

        // Convert cached schema to TableSchema
        let current_schema = TableSchema {
            schema_id: cached_schema.schema_id,
            fields: cached_schema
                .fields
                .iter()
                .map(|f| SchemaFieldInfo {
                    id: f.id,
                    name: f.name.clone(),
                    field_type: f.field_type.clone(),
                    required: f.required,
                    doc: None,
                })
                .collect(),
        };

        // Create evolver with config
        let evolver = match evolution_config {
            Some(config) => SchemaEvolver::new(config),
            None => SchemaEvolver::with_defaults(),
        };

        // Process batch and get evolution plan
        evolver.process_batch(&current_schema, records)
    }

    /// Get the evolved schema with new fields added.
    ///
    /// This is useful for creating Arrow schemas that include newly detected fields.
    pub fn get_evolved_schema(&self, plan: &SchemaEvolutionPlan) -> Result<TableSchema> {
        let cached_schema = self.cache.get_schema().ok_or_else(|| {
            Error::Iceberg(crate::IcebergError::Other(
                "No schema in cache - table not loaded".to_string(),
            ))
        })?;

        let mut fields: Vec<SchemaFieldInfo> = cached_schema
            .fields
            .iter()
            .map(|f| SchemaFieldInfo {
                id: f.id,
                name: f.name.clone(),
                field_type: f.field_type.clone(),
                required: f.required,
                doc: None,
            })
            .collect();

        // Add new fields from plan
        fields.extend(plan.fields_to_add.clone());

        Ok(TableSchema {
            schema_id: plan.new_schema_id,
            fields,
        })
    }
}

/// Types of schema mismatches.
#[derive(Debug, Clone)]
pub enum SchemaMismatch {
    /// Expected field is missing from existing schema
    MissingField { name: String },
    /// Field exists but has different type
    TypeMismatch {
        name: String,
        expected: String,
        actual: String,
    },
    /// Field exists but has different nullability
    NullabilityMismatch {
        name: String,
        expected_required: bool,
        actual_required: bool,
    },
    /// Extra field exists in existing schema (not in expected)
    ExtraField { name: String },
}

/// Builder for creating TableManager instances.
pub struct TableManagerBuilder {
    catalog: Option<Arc<dyn CatalogOperations>>,
    namespace: Option<String>,
    table_name: Option<String>,
    config: TableManagementConfig,
    cache: Option<Arc<MetadataCache>>,
}

impl TableManagerBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            catalog: None,
            namespace: None,
            table_name: None,
            config: TableManagementConfig::default(),
            cache: None,
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
    pub fn config(mut self, config: TableManagementConfig) -> Self {
        self.config = config;
        self
    }

    /// Set auto-create behavior.
    pub fn auto_create(mut self, auto_create: bool) -> Self {
        self.config.auto_create_if_missing = auto_create;
        self
    }

    /// Set schema mismatch action.
    pub fn schema_mismatch_action(mut self, action: SchemaMismatchAction) -> Self {
        self.config.schema_mismatch_action = action;
        self
    }

    /// Set the metadata cache.
    pub fn cache(mut self, cache: Arc<MetadataCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Build the TableManager.
    pub fn build(self) -> Result<TableManager> {
        let catalog = self
            .catalog
            .ok_or_else(|| Error::Config("TableManager requires a catalog".to_string()))?;
        let namespace = self
            .namespace
            .ok_or_else(|| Error::Config("TableManager requires a namespace".to_string()))?;
        let table_name = self
            .table_name
            .ok_or_else(|| Error::Config("TableManager requires a table name".to_string()))?;
        let cache = self.cache.unwrap_or_else(|| Arc::new(MetadataCache::new()));

        Ok(TableManager::new(
            catalog,
            namespace,
            table_name,
            self.config,
            cache,
        ))
    }
}

impl Default for TableManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CatalogType;
    use crate::iceberg::factory::{CatalogHealth, SnapshotCommit, SnapshotCommitResult};
    use async_trait::async_trait;
    use std::collections::HashMap;

    /// Mock catalog for testing.
    struct MockCatalog {
        tables: std::sync::RwLock<HashMap<String, TableInfo>>,
        namespaces: std::sync::RwLock<std::collections::HashSet<String>>,
    }

    impl MockCatalog {
        fn new() -> Self {
            Self {
                tables: std::sync::RwLock::new(HashMap::new()),
                namespaces: std::sync::RwLock::new(std::collections::HashSet::new()),
            }
        }

        fn with_table(table_info: TableInfo) -> Self {
            let mock = Self::new();
            let key = format!("{}.{}", table_info.namespace, table_info.name);
            mock.tables.write().unwrap().insert(key, table_info.clone());
            mock.namespaces
                .write()
                .unwrap()
                .insert(table_info.namespace.clone());
            mock
        }
    }

    #[async_trait]
    impl CatalogOperations for MockCatalog {
        async fn health_check(&self) -> Result<CatalogHealth> {
            Ok(CatalogHealth {
                is_healthy: true,
                response_time_ms: 10,
                message: None,
                catalog_type: CatalogType::Rest,
            })
        }

        async fn list_namespaces(&self) -> Result<Vec<String>> {
            Ok(self.namespaces.read().unwrap().iter().cloned().collect())
        }

        async fn namespace_exists(&self, namespace: &str) -> Result<bool> {
            Ok(self.namespaces.read().unwrap().contains(namespace))
        }

        async fn create_namespace(&self, namespace: &str) -> Result<()> {
            self.namespaces
                .write()
                .unwrap()
                .insert(namespace.to_string());
            Ok(())
        }

        async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
            let tables = self.tables.read().unwrap();
            Ok(tables
                .values()
                .filter(|t| t.namespace == namespace)
                .map(|t| t.name.clone())
                .collect())
        }

        async fn table_exists(&self, namespace: &str, table: &str) -> Result<bool> {
            let key = format!("{}.{}", namespace, table);
            Ok(self.tables.read().unwrap().contains_key(&key))
        }

        async fn load_table(&self, namespace: &str, table: &str) -> Result<TableInfo> {
            let key = format!("{}.{}", namespace, table);
            self.tables
                .read()
                .unwrap()
                .get(&key)
                .cloned()
                .ok_or_else(|| Error::Iceberg(crate::IcebergError::TableNotFound(key)))
        }

        async fn create_table(
            &self,
            namespace: &str,
            table: &str,
            schema: &TableSchema,
        ) -> Result<TableInfo> {
            let table_info = TableInfo {
                namespace: namespace.to_string(),
                name: table.to_string(),
                location: format!("s3://warehouse/{}/{}", namespace, table),
                current_snapshot_id: None,
                schema: schema.clone(),
                properties: HashMap::new(),
            };

            let key = format!("{}.{}", namespace, table);
            self.tables.write().unwrap().insert(key, table_info.clone());

            Ok(table_info)
        }

        async fn current_snapshot_id(&self, namespace: &str, table: &str) -> Result<Option<i64>> {
            let key = format!("{}.{}", namespace, table);
            Ok(self
                .tables
                .read()
                .unwrap()
                .get(&key)
                .and_then(|t| t.current_snapshot_id))
        }

        async fn commit_snapshot(
            &self,
            _namespace: &str,
            _table: &str,
            commit: SnapshotCommit,
        ) -> Result<SnapshotCommitResult> {
            Ok(SnapshotCommitResult {
                snapshot_id: chrono::Utc::now().timestamp_millis(),
                committed_at: chrono::Utc::now(),
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

    fn create_test_schema() -> TableSchema {
        TableSchema {
            schema_id: 1,
            fields: vec![
                SchemaFieldInfo {
                    id: 1,
                    name: "id".to_string(),
                    field_type: "long".to_string(),
                    required: true,
                    doc: None,
                },
                SchemaFieldInfo {
                    id: 2,
                    name: "name".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    doc: None,
                },
            ],
        }
    }

    #[tokio::test]
    async fn test_table_manager_creation() {
        let catalog = Arc::new(MockCatalog::new());
        let cache = Arc::new(MetadataCache::new());

        let manager = TableManager::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            TableManagementConfig::default(),
            cache,
        );

        assert_eq!(manager.full_table_name(), "test_db.test_table");
        assert_eq!(manager.namespace(), "test_db");
        assert_eq!(manager.table_name(), "test_table");
    }

    #[tokio::test]
    async fn test_get_or_create_new_table() {
        let catalog = Arc::new(MockCatalog::new());
        let cache = Arc::new(MetadataCache::new());

        let config = TableManagementConfig {
            auto_create_if_missing: true,
            ..Default::default()
        };

        let manager = TableManager::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            config,
            cache,
        );

        let schema = create_test_schema();
        let result = manager.get_or_create(&schema).await;

        assert!(result.is_ok());
        let table_info = result.unwrap();
        assert_eq!(table_info.namespace, "test_db");
        assert_eq!(table_info.name, "test_table");
    }

    #[tokio::test]
    async fn test_get_or_create_existing_table() {
        let schema = create_test_schema();
        let existing_table = TableInfo {
            namespace: "test_db".to_string(),
            name: "test_table".to_string(),
            location: "s3://warehouse/test_db/test_table".to_string(),
            current_snapshot_id: Some(123),
            schema: schema.clone(),
            properties: HashMap::new(),
        };

        let catalog = Arc::new(MockCatalog::with_table(existing_table));
        let cache = Arc::new(MetadataCache::new());

        let manager = TableManager::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            TableManagementConfig::default(),
            cache,
        );

        let result = manager.get_or_create(&schema).await;

        assert!(result.is_ok());
        let table_info = result.unwrap();
        assert_eq!(table_info.current_snapshot_id, Some(123));
    }

    #[tokio::test]
    async fn test_get_or_create_fails_when_auto_create_disabled() {
        let catalog = Arc::new(MockCatalog::new());
        let cache = Arc::new(MetadataCache::new());

        let config = TableManagementConfig {
            auto_create_if_missing: false,
            ..Default::default()
        };

        let manager = TableManager::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            config,
            cache,
        );

        let schema = create_test_schema();
        let result = manager.get_or_create(&schema).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_schema_validation_matching() {
        let schema = create_test_schema();
        let existing_table = TableInfo {
            namespace: "test_db".to_string(),
            name: "test_table".to_string(),
            location: "s3://warehouse/test_db/test_table".to_string(),
            current_snapshot_id: Some(123),
            schema: schema.clone(),
            properties: HashMap::new(),
        };

        let catalog = Arc::new(MockCatalog::with_table(existing_table));
        let cache = Arc::new(MetadataCache::new());

        let config = TableManagementConfig {
            schema_mismatch_action: SchemaMismatchAction::Fail,
            ..Default::default()
        };

        let manager = TableManager::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            config,
            cache,
        );

        let result = manager.get_or_create(&schema).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_schema_validation_mismatch_fail() {
        let existing_schema = create_test_schema();
        let existing_table = TableInfo {
            namespace: "test_db".to_string(),
            name: "test_table".to_string(),
            location: "s3://warehouse/test_db/test_table".to_string(),
            current_snapshot_id: Some(123),
            schema: existing_schema,
            properties: HashMap::new(),
        };

        let catalog = Arc::new(MockCatalog::with_table(existing_table));
        let cache = Arc::new(MetadataCache::new());

        let config = TableManagementConfig {
            schema_mismatch_action: SchemaMismatchAction::Fail,
            ..Default::default()
        };

        let manager = TableManager::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            config,
            cache,
        );

        // Expected schema has different type
        let expected_schema = TableSchema {
            schema_id: 1,
            fields: vec![SchemaFieldInfo {
                id: 1,
                name: "id".to_string(),
                field_type: "string".to_string(), // Different type!
                required: true,
                doc: None,
            }],
        };

        let result = manager.get_or_create(&expected_schema).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_schema_validation_mismatch_warn() {
        let existing_schema = create_test_schema();
        let existing_table = TableInfo {
            namespace: "test_db".to_string(),
            name: "test_table".to_string(),
            location: "s3://warehouse/test_db/test_table".to_string(),
            current_snapshot_id: Some(123),
            schema: existing_schema,
            properties: HashMap::new(),
        };

        let catalog = Arc::new(MockCatalog::with_table(existing_table));
        let cache = Arc::new(MetadataCache::new());

        let config = TableManagementConfig {
            schema_mismatch_action: SchemaMismatchAction::Warn,
            ..Default::default()
        };

        let manager = TableManager::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            config,
            cache,
        );

        // Expected schema has different type
        let expected_schema = TableSchema {
            schema_id: 1,
            fields: vec![SchemaFieldInfo {
                id: 1,
                name: "id".to_string(),
                field_type: "string".to_string(), // Different type!
                required: true,
                doc: None,
            }],
        };

        // Should succeed with warning
        let result = manager.get_or_create(&expected_schema).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cache_update() {
        let schema = create_test_schema();
        let existing_table = TableInfo {
            namespace: "test_db".to_string(),
            name: "test_table".to_string(),
            location: "s3://warehouse/test_db/test_table".to_string(),
            current_snapshot_id: Some(123),
            schema: schema.clone(),
            properties: HashMap::new(),
        };

        let catalog = Arc::new(MockCatalog::with_table(existing_table));
        let cache = Arc::new(MetadataCache::new());

        let manager = TableManager::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            TableManagementConfig::default(),
            cache.clone(),
        );

        // Cache should be empty initially
        assert!(cache.get_snapshot_id().is_none());

        // Load table
        let _ = manager.load_table().await;

        // Cache should be populated
        assert_eq!(cache.get_snapshot_id(), Some(123));
        assert!(cache.get_schema().is_some());
        assert!(cache.get_table_location().is_some());
    }

    #[tokio::test]
    async fn test_current_snapshot_id_from_cache() {
        let schema = create_test_schema();
        let existing_table = TableInfo {
            namespace: "test_db".to_string(),
            name: "test_table".to_string(),
            location: "s3://warehouse/test_db/test_table".to_string(),
            current_snapshot_id: Some(123),
            schema: schema.clone(),
            properties: HashMap::new(),
        };

        let catalog = Arc::new(MockCatalog::with_table(existing_table));
        let cache = Arc::new(MetadataCache::new());

        let manager = TableManager::new(
            catalog,
            "test_db".to_string(),
            "test_table".to_string(),
            TableManagementConfig::default(),
            cache.clone(),
        );

        // First call fetches from catalog
        let snapshot_id = manager.current_snapshot_id().await.unwrap();
        assert_eq!(snapshot_id, Some(123));

        // Should be cached now
        assert_eq!(cache.get_snapshot_id(), Some(123));
    }

    #[tokio::test]
    async fn test_builder_pattern() {
        let catalog = Arc::new(MockCatalog::new());
        let cache = Arc::new(MetadataCache::new());

        let manager = TableManagerBuilder::new()
            .catalog(catalog)
            .namespace("my_db")
            .table_name("my_table")
            .auto_create(true)
            .schema_mismatch_action(SchemaMismatchAction::Warn)
            .cache(cache)
            .build()
            .unwrap();

        assert_eq!(manager.full_table_name(), "my_db.my_table");
    }

    #[tokio::test]
    async fn test_builder_missing_catalog() {
        let result = TableManagerBuilder::new()
            .namespace("my_db")
            .table_name("my_table")
            .build();

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_exists() {
        let schema = create_test_schema();
        let existing_table = TableInfo {
            namespace: "test_db".to_string(),
            name: "existing_table".to_string(),
            location: "s3://warehouse/test_db/existing_table".to_string(),
            current_snapshot_id: None,
            schema,
            properties: HashMap::new(),
        };

        let catalog = Arc::new(MockCatalog::with_table(existing_table));
        let cache = Arc::new(MetadataCache::new());

        // Check existing table
        let manager = TableManager::new(
            catalog.clone(),
            "test_db".to_string(),
            "existing_table".to_string(),
            TableManagementConfig::default(),
            cache.clone(),
        );
        assert!(manager.exists().await.unwrap());

        // Check non-existing table
        let manager2 = TableManager::new(
            catalog,
            "test_db".to_string(),
            "non_existing_table".to_string(),
            TableManagementConfig::default(),
            cache,
        );
        assert!(!manager2.exists().await.unwrap());
    }

    #[test]
    fn test_find_schema_mismatches() {
        let catalog = Arc::new(MockCatalog::new());
        let cache = Arc::new(MetadataCache::new());

        let manager = TableManager::new(
            catalog,
            "test".to_string(),
            "table".to_string(),
            TableManagementConfig::default(),
            cache,
        );

        let existing = TableSchema {
            schema_id: 1,
            fields: vec![
                SchemaFieldInfo {
                    id: 1,
                    name: "id".to_string(),
                    field_type: "long".to_string(),
                    required: true,
                    doc: None,
                },
                SchemaFieldInfo {
                    id: 2,
                    name: "extra".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    doc: None,
                },
            ],
        };

        let expected = TableSchema {
            schema_id: 1,
            fields: vec![
                SchemaFieldInfo {
                    id: 1,
                    name: "id".to_string(),
                    field_type: "string".to_string(), // Type mismatch
                    required: true,
                    doc: None,
                },
                SchemaFieldInfo {
                    id: 3,
                    name: "missing".to_string(), // Missing from existing
                    field_type: "int".to_string(),
                    required: true,
                    doc: None,
                },
            ],
        };

        let mismatches = manager.find_schema_mismatches(&existing, &expected);

        // Should find: type mismatch for 'id', missing 'missing', extra 'extra'
        assert_eq!(mismatches.len(), 3);
    }
}
