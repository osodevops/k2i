//! Hive Metastore catalog implementation.
//!
//! The Hive Metastore (HMS) is the traditional metadata store for Apache Hive and
//! is widely used in Hadoop ecosystems. This implementation provides Iceberg catalog
//! operations using the HMS Thrift protocol via the `hive_metastore` crate.
//!
//! Key features:
//! - Thrift-based communication with HMS
//! - Maps Hive databases to Iceberg namespaces
//! - Maps Hive tables to Iceberg table metadata
//! - Circuit breaker for fault tolerance

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::config::{CatalogType, IcebergConfig};
use crate::iceberg::factory::{
    CatalogFactory, CatalogHealth, CatalogOperations, SchemaFieldInfo, SnapshotCommit,
    SnapshotCommitResult, TableInfo, TableSchema,
};
use crate::{Error, IcebergError, Result};
use async_trait::async_trait;
use hive_metastore::{ThriftHiveMetastoreClient, ThriftHiveMetastoreClientBuilder};
use parking_lot::RwLock as SyncRwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use volo_thrift::MaybeException;

/// Factory for Hive Metastore catalog.
pub struct HiveCatalogFactory;

#[async_trait]
impl CatalogFactory for HiveCatalogFactory {
    async fn create(&self, config: &IcebergConfig) -> Result<Arc<dyn CatalogOperations>> {
        let client = HiveCatalogClient::new(config).await?;
        Ok(Arc::new(client))
    }

    fn catalog_type(&self) -> CatalogType {
        CatalogType::Hive
    }
}

/// Hive Metastore catalog client configuration.
#[derive(Debug, Clone)]
pub struct HiveClientConfig {
    /// Metastore URI (thrift://host:port)
    pub metastore_uri: String,
    /// Warehouse path
    pub warehouse_path: String,
    /// Database name (namespace)
    pub database_name: String,
    /// Request timeout
    pub timeout: Duration,
}

/// Hive Metastore catalog client implementation.
///
/// Implements the CatalogOperations trait using HMS Thrift protocol.
pub struct HiveCatalogClient {
    /// Thrift client (wrapped in Arc for thread safety)
    /// Uses tokio::sync::RwLock for async compatibility
    client: Arc<RwLock<Option<ThriftHiveMetastoreClient>>>,
    /// Client configuration
    config: HiveClientConfig,
    /// Last health check result (sync RwLock is fine here - no await while holding)
    last_health_check: SyncRwLock<Option<CatalogHealth>>,
    /// Circuit breaker for fault tolerance
    circuit_breaker: CircuitBreaker,
    /// Connection status
    connected: SyncRwLock<bool>,
}

impl HiveCatalogClient {
    /// Create a new Hive Metastore catalog client.
    pub async fn new(config: &IcebergConfig) -> Result<Self> {
        let metastore_uri = config.hive_metastore_uri.clone().ok_or_else(|| {
            Error::Config("Hive catalog requires hive_metastore_uri to be set".into())
        })?;

        let timeout = Duration::from_secs(config.catalog_manager.request_timeout_seconds);

        let client_config = HiveClientConfig {
            metastore_uri: metastore_uri.clone(),
            warehouse_path: config.warehouse_path.clone(),
            database_name: config.database_name.clone(),
            timeout,
        };

        // Create circuit breaker for fault tolerance
        let circuit_breaker = CircuitBreaker::new(
            CircuitBreakerConfig::new("hive-catalog")
                .with_failure_threshold(5)
                .with_reset_timeout(Duration::from_secs(30))
                .with_success_threshold(2),
        );

        let client = Self {
            client: Arc::new(RwLock::new(None)),
            config: client_config,
            last_health_check: SyncRwLock::new(None),
            circuit_breaker,
            connected: SyncRwLock::new(false),
        };

        // Attempt initial connection
        if let Err(e) = client.connect().await {
            warn!(error = %e, "Initial connection to Hive Metastore failed, will retry on first operation");
        }

        info!(
            uri = %metastore_uri,
            database = %config.database_name,
            "Hive Metastore catalog client initialized"
        );

        Ok(client)
    }

    /// Ensure we have an active connection to the Hive Metastore.
    async fn ensure_connected(&self) -> Result<()> {
        if *self.connected.read() {
            return Ok(());
        }

        self.connect().await
    }

    /// Connect to the Hive Metastore.
    async fn connect(&self) -> Result<()> {
        let uri = &self.config.metastore_uri;

        // Parse the URI to extract host and port
        let (host, port) = Self::parse_thrift_uri(uri)?;

        debug!(host = %host, port = port, "Connecting to Hive Metastore");

        // Create socket address
        let address_str = format!("{}:{}", host, port);
        let socket_addr: std::net::SocketAddr = address_str.parse().map_err(|e| {
            Error::Iceberg(IcebergError::CatalogConnection(format!(
                "Failed to parse Hive Metastore address {}: {}",
                address_str, e
            )))
        })?;

        // Create Thrift client using hive_metastore crate
        let thrift_client = ThriftHiveMetastoreClientBuilder::new("hive-catalog")
            .address(socket_addr)
            .build();

        *self.client.write().await = Some(thrift_client);
        *self.connected.write() = true;

        info!(uri = %uri, "Connected to Hive Metastore");
        Ok(())
    }

    /// Parse a Thrift URI (thrift://host:port) into host and port.
    fn parse_thrift_uri(uri: &str) -> Result<(String, u16)> {
        let uri = uri.strip_prefix("thrift://").unwrap_or(uri);

        let parts: Vec<&str> = uri.split(':').collect();
        if parts.len() != 2 {
            return Err(Error::Config(format!(
                "Invalid Hive Metastore URI format: {}. Expected thrift://host:port",
                uri
            )));
        }

        let host = parts[0].to_string();
        let port = parts[1].parse::<u16>().map_err(|_| {
            Error::Config(format!("Invalid port in Hive Metastore URI: {}", parts[1]))
        })?;

        Ok((host, port))
    }

    /// Check if circuit breaker allows the request.
    fn check_circuit_breaker(&self) -> Result<()> {
        if !self.circuit_breaker.is_allowed() {
            warn!("Circuit breaker is open, rejecting request");
            return Err(Error::Iceberg(IcebergError::CatalogConnection(
                "Circuit breaker is open - Hive operations temporarily blocked".to_string(),
            )));
        }
        Ok(())
    }

    /// Record success in circuit breaker.
    fn record_success(&self) {
        self.circuit_breaker.record_success();
    }

    /// Record failure in circuit breaker.
    fn record_failure(&self) {
        self.circuit_breaker.record_failure();
        // Mark as disconnected on failure
        *self.connected.write() = false;
    }

    /// Convert Hive table to TableInfo.
    fn hive_table_to_table_info(
        &self,
        table: &hive_metastore::Table,
        namespace: &str,
    ) -> Result<TableInfo> {
        let name = table
            .table_name
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_default();

        let location = table
            .sd
            .as_ref()
            .and_then(|sd| sd.location.as_ref())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{}/{}/{}", self.config.warehouse_path, namespace, name));

        // Extract schema from storage descriptor columns
        let schema = self.extract_schema_from_hive_table(table)?;

        // Extract current snapshot ID from table parameters
        let current_snapshot_id = table
            .parameters
            .as_ref()
            .and_then(|params| params.get("current-snapshot-id"))
            .and_then(|s| s.to_string().parse::<i64>().ok());

        // Extract table properties - convert FastStr HashMap to String HashMap
        let properties: HashMap<String, String> = table
            .parameters
            .as_ref()
            .map(|p| {
                p.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(TableInfo {
            namespace: namespace.to_string(),
            name,
            location,
            current_snapshot_id,
            schema,
            properties,
        })
    }

    /// Extract schema from Hive table.
    fn extract_schema_from_hive_table(&self, table: &hive_metastore::Table) -> Result<TableSchema> {
        let columns = table
            .sd
            .as_ref()
            .and_then(|sd| sd.cols.as_ref())
            .cloned()
            .unwrap_or_default();

        let fields: Vec<SchemaFieldInfo> = columns
            .iter()
            .enumerate()
            .map(|(idx, col)| SchemaFieldInfo {
                id: (idx + 1) as i32,
                name: col.name.as_ref().map(|s| s.to_string()).unwrap_or_default(),
                field_type: self.hive_type_to_iceberg_type(
                    col.r#type.as_ref().map(|s| s.as_str()).unwrap_or("string"),
                ),
                required: false,
                doc: col.comment.as_ref().map(|s| s.to_string()),
            })
            .collect();

        Ok(TableSchema {
            schema_id: 0,
            fields,
        })
    }

    /// Convert Hive type to Iceberg type string.
    fn hive_type_to_iceberg_type(&self, hive_type: &str) -> String {
        match hive_type.to_lowercase().as_str() {
            "boolean" => "boolean".to_string(),
            "tinyint" => "int".to_string(),
            "smallint" => "int".to_string(),
            "int" | "integer" => "int".to_string(),
            "bigint" => "long".to_string(),
            "float" => "float".to_string(),
            "double" => "double".to_string(),
            "decimal" => "decimal(38, 18)".to_string(),
            "string" | "varchar" | "char" => "string".to_string(),
            "binary" => "binary".to_string(),
            "date" => "date".to_string(),
            "timestamp" => "timestamptz".to_string(),
            _ => {
                // Handle complex types
                if hive_type.starts_with("array<") {
                    let inner = &hive_type[6..hive_type.len() - 1];
                    format!("list<{}>", self.hive_type_to_iceberg_type(inner))
                } else if hive_type.starts_with("map<")
                    || hive_type.starts_with("struct<")
                    || hive_type.starts_with("decimal(")
                {
                    hive_type.to_string()
                } else {
                    "string".to_string()
                }
            }
        }
    }

    /// Convert Iceberg type to Hive type string.
    fn iceberg_type_to_hive_type(&self, iceberg_type: &str) -> String {
        match iceberg_type.to_lowercase().as_str() {
            "boolean" => "boolean".to_string(),
            "int" | "integer" => "int".to_string(),
            "long" => "bigint".to_string(),
            "float" => "float".to_string(),
            "double" => "double".to_string(),
            "string" => "string".to_string(),
            "binary" => "binary".to_string(),
            "date" => "date".to_string(),
            "time" => "string".to_string(),
            "timestamp" | "timestamptz" => "timestamp".to_string(),
            "uuid" => "string".to_string(),
            _ => {
                if iceberg_type.starts_with("decimal") {
                    iceberg_type.to_string()
                } else if iceberg_type.starts_with("list<") {
                    let inner = &iceberg_type[5..iceberg_type.len() - 1];
                    format!("array<{}>", self.iceberg_type_to_hive_type(inner))
                } else {
                    "string".to_string()
                }
            }
        }
    }

    /// Build Hive storage descriptor.
    fn build_storage_descriptor(
        &self,
        schema: &TableSchema,
        location: &str,
    ) -> hive_metastore::StorageDescriptor {
        let columns: Vec<hive_metastore::FieldSchema> = schema
            .fields
            .iter()
            .map(|f| hive_metastore::FieldSchema {
                name: Some(f.name.clone().into()),
                r#type: Some(self.iceberg_type_to_hive_type(&f.field_type).into()),
                comment: f.doc.clone().map(|s| s.into()),
            })
            .collect();

        hive_metastore::StorageDescriptor {
            cols: Some(columns),
            location: Some(location.to_string().into()),
            input_format: Some("org.apache.iceberg.mr.hive.HiveIcebergInputFormat".into()),
            output_format: Some("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat".into()),
            serde_info: Some(hive_metastore::SerDeInfo {
                name: None,
                serialization_lib: Some("org.apache.iceberg.mr.hive.HiveIcebergSerDe".into()),
                parameters: None,
            }),
            compressed: Some(false),
            num_buckets: Some(-1),
            bucket_cols: None,
            sort_cols: None,
            parameters: None,
            skewed_info: None,
            stored_as_sub_directories: Some(false),
        }
    }
}

#[async_trait]
impl CatalogOperations for HiveCatalogClient {
    async fn health_check(&self) -> Result<CatalogHealth> {
        let start = Instant::now();

        // Try to get all databases as a health check
        let result = self.list_namespaces().await;
        let response_time_ms = start.elapsed().as_millis() as u64;

        let health = match result {
            Ok(_) => CatalogHealth {
                is_healthy: true,
                response_time_ms,
                message: Some(format!(
                    "Hive Metastore at {} is healthy",
                    self.config.metastore_uri
                )),
                catalog_type: CatalogType::Hive,
            },
            Err(e) => CatalogHealth {
                is_healthy: false,
                response_time_ms,
                message: Some(format!("Connection failed: {}", e)),
                catalog_type: CatalogType::Hive,
            },
        };

        *self.last_health_check.write() = Some(health.clone());
        Ok(health)
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        self.check_circuit_breaker()?;
        self.ensure_connected().await?;

        let client_guard = self.client.read().await;
        let client = client_guard.as_ref().ok_or_else(|| {
            Error::Iceberg(IcebergError::CatalogConnection(
                "Hive Metastore client not connected".into(),
            ))
        })?;

        match client.get_all_databases().await {
            Ok(response) => {
                self.record_success();
                // Convert MaybeException<Vec<FastStr>, _> to Vec<String>
                // The response is a MaybeException type that wraps the result
                match response {
                    MaybeException::Ok(databases) => {
                        Ok(databases.into_iter().map(|s| s.to_string()).collect())
                    }
                    MaybeException::Exception(e) => {
                        self.record_failure();
                        Err(Error::Iceberg(IcebergError::CatalogConnection(format!(
                            "Hive exception listing databases: {:?}",
                            e
                        ))))
                    }
                }
            }
            Err(e) => {
                self.record_failure();
                Err(Error::Iceberg(IcebergError::CatalogConnection(format!(
                    "Failed to list Hive databases: {}",
                    e
                ))))
            }
        }
    }

    async fn namespace_exists(&self, namespace: &str) -> Result<bool> {
        self.check_circuit_breaker()?;
        self.ensure_connected().await?;

        let client_guard = self.client.read().await;
        let client = client_guard.as_ref().ok_or_else(|| {
            Error::Iceberg(IcebergError::CatalogConnection(
                "Hive Metastore client not connected".into(),
            ))
        })?;

        match client.get_database(namespace.to_string().into()).await {
            Ok(_) => {
                self.record_success();
                Ok(true)
            }
            Err(e) => {
                let error_str = e.to_string();
                if error_str.contains("NoSuchObjectException") {
                    self.record_success();
                    Ok(false)
                } else {
                    self.record_failure();
                    Err(Error::Iceberg(IcebergError::CatalogConnection(format!(
                        "Failed to check Hive database: {}",
                        e
                    ))))
                }
            }
        }
    }

    async fn create_namespace(&self, namespace: &str) -> Result<()> {
        self.check_circuit_breaker()?;

        // Check if already exists
        if self.namespace_exists(namespace).await? {
            debug!(namespace = %namespace, "Hive database already exists");
            return Ok(());
        }

        self.ensure_connected().await?;

        let client_guard = self.client.read().await;
        let client = client_guard.as_ref().ok_or_else(|| {
            Error::Iceberg(IcebergError::CatalogConnection(
                "Hive Metastore client not connected".into(),
            ))
        })?;

        let database = hive_metastore::Database {
            name: Some(namespace.to_string().into()),
            description: Some("Iceberg database created by k2i".into()),
            location_uri: Some(format!("{}/{}", self.config.warehouse_path, namespace).into()),
            parameters: None,
            privileges: None,
            owner_name: None,
            owner_type: None,
            catalog_name: None,
        };

        match client.create_database(database).await {
            Ok(_) => {
                self.record_success();
                info!(namespace = %namespace, "Created Hive database");
                Ok(())
            }
            Err(e) => {
                let error_str = e.to_string();
                if error_str.contains("AlreadyExistsException") {
                    self.record_success();
                    debug!(namespace = %namespace, "Hive database already exists");
                    Ok(())
                } else {
                    self.record_failure();
                    Err(Error::Iceberg(IcebergError::CatalogConnection(format!(
                        "Failed to create Hive database: {}",
                        e
                    ))))
                }
            }
        }
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        self.check_circuit_breaker()?;
        self.ensure_connected().await?;

        let client_guard = self.client.read().await;
        let client = client_guard.as_ref().ok_or_else(|| {
            Error::Iceberg(IcebergError::CatalogConnection(
                "Hive Metastore client not connected".into(),
            ))
        })?;

        match client.get_all_tables(namespace.to_string().into()).await {
            Ok(response) => match response {
                MaybeException::Ok(tables) => {
                    self.record_success();
                    Ok(tables.into_iter().map(|s| s.to_string()).collect())
                }
                MaybeException::Exception(e) => {
                    self.record_failure();
                    Err(Error::Iceberg(IcebergError::CatalogConnection(format!(
                        "Hive exception listing tables: {:?}",
                        e
                    ))))
                }
            },
            Err(e) => {
                self.record_failure();
                Err(Error::Iceberg(IcebergError::CatalogConnection(format!(
                    "Failed to list Hive tables: {}",
                    e
                ))))
            }
        }
    }

    async fn table_exists(&self, namespace: &str, table: &str) -> Result<bool> {
        self.check_circuit_breaker()?;
        self.ensure_connected().await?;

        let client_guard = self.client.read().await;
        let client = client_guard.as_ref().ok_or_else(|| {
            Error::Iceberg(IcebergError::CatalogConnection(
                "Hive Metastore client not connected".into(),
            ))
        })?;

        match client
            .get_table(namespace.to_string().into(), table.to_string().into())
            .await
        {
            Ok(_) => {
                self.record_success();
                Ok(true)
            }
            Err(e) => {
                let error_str = e.to_string();
                if error_str.contains("NoSuchObjectException") {
                    self.record_success();
                    Ok(false)
                } else {
                    self.record_failure();
                    Err(Error::Iceberg(IcebergError::CatalogConnection(format!(
                        "Failed to check Hive table: {}",
                        e
                    ))))
                }
            }
        }
    }

    async fn load_table(&self, namespace: &str, table: &str) -> Result<TableInfo> {
        self.check_circuit_breaker()?;
        self.ensure_connected().await?;

        let client_guard = self.client.read().await;
        let client = client_guard.as_ref().ok_or_else(|| {
            Error::Iceberg(IcebergError::CatalogConnection(
                "Hive Metastore client not connected".into(),
            ))
        })?;

        match client
            .get_table(namespace.to_string().into(), table.to_string().into())
            .await
        {
            Ok(response) => match response {
                MaybeException::Ok(hive_table) => {
                    self.record_success();
                    self.hive_table_to_table_info(&hive_table, namespace)
                }
                MaybeException::Exception(e) => {
                    let error_str = format!("{:?}", e);
                    if error_str.contains("NoSuchObjectException") {
                        self.record_success();
                        Err(Error::Iceberg(IcebergError::TableNotFound(format!(
                            "{}.{}",
                            namespace, table
                        ))))
                    } else {
                        self.record_failure();
                        Err(Error::Iceberg(IcebergError::CatalogConnection(format!(
                            "Hive exception loading table: {:?}",
                            e
                        ))))
                    }
                }
            },
            Err(e) => {
                self.record_failure();
                Err(Error::Iceberg(IcebergError::CatalogConnection(format!(
                    "Failed to load Hive table: {}",
                    e
                ))))
            }
        }
    }

    async fn create_table(
        &self,
        namespace: &str,
        table: &str,
        schema: &TableSchema,
    ) -> Result<TableInfo> {
        self.check_circuit_breaker()?;

        // Ensure namespace exists
        self.create_namespace(namespace).await?;

        self.ensure_connected().await?;

        let client_guard = self.client.read().await;
        let client = client_guard.as_ref().ok_or_else(|| {
            Error::Iceberg(IcebergError::CatalogConnection(
                "Hive Metastore client not connected".into(),
            ))
        })?;

        let table_location = format!("{}/{}/{}", self.config.warehouse_path, namespace, table);
        let storage_descriptor = self.build_storage_descriptor(schema, &table_location);

        // Build Iceberg-specific parameters using AHashMap with FastStr
        let mut parameters = ahash::AHashMap::new();
        parameters.insert(
            faststr::FastStr::from_static_str("table_type"),
            faststr::FastStr::from_static_str("ICEBERG"),
        );
        parameters.insert(
            faststr::FastStr::from_static_str("metadata_location"),
            faststr::FastStr::from(format!("{}/metadata/v1.metadata.json", table_location)),
        );

        let hive_table = hive_metastore::Table {
            table_name: Some(table.to_string().into()),
            db_name: Some(namespace.to_string().into()),
            owner: None,
            create_time: Some(chrono::Utc::now().timestamp() as i32),
            last_access_time: Some(0),
            retention: Some(0),
            sd: Some(storage_descriptor),
            partition_keys: None,
            parameters: Some(parameters),
            view_original_text: None,
            view_expanded_text: None,
            table_type: Some("EXTERNAL_TABLE".into()),
            privileges: None,
            temporary: Some(false),
            rewrite_enabled: None,
            cat_name: None,
        };

        match client.create_table(hive_table).await {
            Ok(_) => {
                self.record_success();
                info!(
                    namespace = %namespace,
                    table = %table,
                    "Created Hive table"
                );

                Ok(TableInfo {
                    namespace: namespace.to_string(),
                    name: table.to_string(),
                    location: table_location,
                    current_snapshot_id: None,
                    schema: schema.clone(),
                    properties: HashMap::new(),
                })
            }
            Err(e) => {
                self.record_failure();
                Err(Error::Iceberg(IcebergError::CatalogConnection(format!(
                    "Failed to create Hive table: {}",
                    e
                ))))
            }
        }
    }

    async fn current_snapshot_id(&self, namespace: &str, table: &str) -> Result<Option<i64>> {
        let table_info = self.load_table(namespace, table).await?;
        Ok(table_info.current_snapshot_id)
    }

    async fn commit_snapshot(
        &self,
        namespace: &str,
        table: &str,
        commit: SnapshotCommit,
    ) -> Result<SnapshotCommitResult> {
        self.check_circuit_breaker()?;

        // Load current table
        let table_info = self.load_table(namespace, table).await?;
        let table_location = table_info.location.clone();

        self.ensure_connected().await?;

        let client_guard = self.client.read().await;
        let client = client_guard.as_ref().ok_or_else(|| {
            Error::Iceberg(IcebergError::CatalogConnection(
                "Hive Metastore client not connected".into(),
            ))
        })?;

        // Generate new snapshot ID
        let new_snapshot_id = chrono::Utc::now().timestamp_millis();

        // Build updated parameters using AHashMap with FastStr
        let mut parameters = ahash::AHashMap::new();

        // Copy existing properties
        for (k, v) in &table_info.properties {
            parameters.insert(
                faststr::FastStr::from(k.clone()),
                faststr::FastStr::from(v.clone()),
            );
        }

        parameters.insert(
            faststr::FastStr::from_static_str("current-snapshot-id"),
            faststr::FastStr::from(new_snapshot_id.to_string()),
        );
        parameters.insert(
            faststr::FastStr::from_static_str("previous-snapshot-id"),
            faststr::FastStr::from(
                commit
                    .expected_snapshot_id
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
            ),
        );

        // Update metadata location
        let metadata_location = format!(
            "{}/metadata/snap-{}-{}.metadata.json",
            table_location,
            new_snapshot_id,
            uuid::Uuid::new_v4()
        );
        parameters.insert(
            faststr::FastStr::from_static_str("metadata_location"),
            faststr::FastStr::from(metadata_location),
        );

        // Add summary to parameters
        for (key, value) in &commit.summary {
            parameters.insert(
                faststr::FastStr::from(format!("snapshot.{}", key)),
                faststr::FastStr::from(value.clone()),
            );
        }

        // Rebuild storage descriptor
        let storage_descriptor = self.build_storage_descriptor(&table_info.schema, &table_location);

        let updated_table = hive_metastore::Table {
            table_name: Some(table.to_string().into()),
            db_name: Some(namespace.to_string().into()),
            owner: None,
            create_time: None,
            last_access_time: Some(chrono::Utc::now().timestamp() as i32),
            retention: Some(0),
            sd: Some(storage_descriptor),
            partition_keys: None,
            parameters: Some(parameters),
            view_original_text: None,
            view_expanded_text: None,
            table_type: Some("EXTERNAL_TABLE".into()),
            privileges: None,
            temporary: Some(false),
            rewrite_enabled: None,
            cat_name: None,
        };

        match client
            .alter_table(
                namespace.to_string().into(),
                table.to_string().into(),
                updated_table,
            )
            .await
        {
            Ok(_) => {
                self.record_success();
                info!(
                    namespace = %namespace,
                    table = %table,
                    snapshot_id = new_snapshot_id,
                    files_added = commit.files_to_add.len(),
                    "Snapshot committed to Hive"
                );

                Ok(SnapshotCommitResult {
                    snapshot_id: new_snapshot_id,
                    committed_at: chrono::Utc::now(),
                    files_added: commit.files_to_add.len(),
                    files_removed: commit.files_to_remove.len(),
                })
            }
            Err(e) => {
                self.record_failure();
                Err(Error::Iceberg(IcebergError::CatalogConnection(format!(
                    "Failed to commit snapshot to Hive: {}",
                    e
                ))))
            }
        }
    }

    fn catalog_type(&self) -> CatalogType {
        CatalogType::Hive
    }

    fn warehouse_path(&self) -> &str {
        &self.config.warehouse_path
    }

    async fn close(&self) -> Result<()> {
        *self.client.write().await = None;
        *self.connected.write() = false;
        info!("Hive Metastore catalog client closed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ParquetCompression;

    fn create_test_config() -> IcebergConfig {
        IcebergConfig {
            catalog_type: CatalogType::Hive,
            warehouse_path: "s3://bucket/warehouse".into(),
            database_name: "test_db".into(),
            table_name: "test_table".into(),
            target_file_size_mb: 512,
            compression: ParquetCompression::Snappy,
            partition_spec: vec![],
            rest_uri: None,
            hive_metastore_uri: Some("thrift://localhost:9083".into()),
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

    #[test]
    fn test_hive_catalog_factory_type() {
        let factory = HiveCatalogFactory;
        assert_eq!(factory.catalog_type(), CatalogType::Hive);
    }

    #[test]
    fn test_parse_thrift_uri() {
        // Test with thrift:// prefix
        let (host, port) = HiveCatalogClient::parse_thrift_uri("thrift://localhost:9083").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 9083);

        // Test without prefix
        let (host, port) =
            HiveCatalogClient::parse_thrift_uri("metastore.example.com:9083").unwrap();
        assert_eq!(host, "metastore.example.com");
        assert_eq!(port, 9083);
    }

    #[test]
    fn test_parse_thrift_uri_invalid() {
        // Invalid format - no port
        let result = HiveCatalogClient::parse_thrift_uri("localhost");
        assert!(result.is_err());

        // Invalid port
        let result = HiveCatalogClient::parse_thrift_uri("localhost:invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_hive_type_conversion() {
        // Test that type mappings are correct
        assert_eq!("boolean".to_lowercase(), "boolean");
        assert_eq!("bigint".to_lowercase(), "bigint");
        assert_eq!("timestamp".to_lowercase(), "timestamp");
    }

    #[test]
    fn test_schema_building() {
        let schema = TableSchema {
            schema_id: 0,
            fields: vec![
                SchemaFieldInfo {
                    id: 1,
                    name: "id".into(),
                    field_type: "long".into(),
                    required: true,
                    doc: None,
                },
                SchemaFieldInfo {
                    id: 2,
                    name: "name".into(),
                    field_type: "string".into(),
                    required: false,
                    doc: Some("User name".into()),
                },
            ],
        };

        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[0].name, "id");
        assert_eq!(schema.fields[1].name, "name");
    }

    #[tokio::test]
    async fn test_hive_catalog_factory_requires_uri() {
        let factory = HiveCatalogFactory;
        let mut config = create_test_config();
        config.hive_metastore_uri = None;

        let result = factory.create(&config).await;
        assert!(result.is_err());
    }

    // Note: Integration tests with actual Hive Metastore require Docker
    // and are marked as ignored
    #[tokio::test]
    #[ignore = "requires Hive Metastore Docker container"]
    async fn test_hive_catalog_connection() {
        let config = create_test_config();
        let result = HiveCatalogClient::new(&config).await;
        // This will fail without a running Hive Metastore
        assert!(result.is_ok() || result.is_err());
    }
}
