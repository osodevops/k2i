//! Catalog factory for creating catalog instances.
//!
//! This module provides a factory pattern for creating different Iceberg catalog
//! implementations based on configuration. Supported catalog types:
//!
//! - REST Catalog (via `iceberg-catalog-rest`)
//! - AWS Glue Catalog (planned)
//! - Hive Metastore (planned)
//! - Nessie (planned)

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::config::{CatalogType, CredentialType, IcebergConfig};
use crate::iceberg::rest_api;
use crate::{Error, IcebergError, Result};
use async_trait::async_trait;
use parking_lot::RwLock;
use reqwest::{Client, StatusCode};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Schema ID
    pub schema_id: i32,
    /// Schema fields
    pub fields: Vec<SchemaFieldInfo>,
}

/// Schema field information.
#[derive(Debug, Clone)]
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
        let rest_uri = config
            .rest_uri
            .as_ref()
            .ok_or_else(|| Error::Config("REST catalog requires rest_uri to be set".into()))?;

        let client = RestCatalogClient::new(config, rest_uri.clone()).await?;
        Ok(Arc::new(client))
    }

    fn catalog_type(&self) -> CatalogType {
        CatalogType::Rest
    }
}

/// REST catalog client configuration.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RestClientConfig {
    /// REST API base URI
    pub base_uri: String,
    /// Request timeout
    pub timeout: Duration,
    /// Maximum retries for transient errors
    pub max_retries: u32,
    /// Credential type
    pub credential_type: CredentialType,
    /// Bearer token (if using bearer auth)
    pub bearer_token: Option<String>,
    /// OAuth2 client ID (if using OAuth2)
    pub oauth2_client_id: Option<String>,
    /// OAuth2 client secret (if using OAuth2)
    pub oauth2_client_secret: Option<String>,
    /// OAuth2 scope (if using OAuth2)
    pub oauth2_scope: Option<String>,
    /// Warehouse path
    pub warehouse_path: String,
}

/// REST catalog client implementation.
///
/// Implements the Apache Iceberg REST Catalog API specification.
pub struct RestCatalogClient {
    /// HTTP client
    client: Client,
    /// Client configuration
    config: RestClientConfig,
    /// Cached OAuth2 token
    oauth_token: RwLock<Option<CachedToken>>,
    /// Last health check result
    last_health_check: RwLock<Option<CatalogHealth>>,
    /// Circuit breaker for fault tolerance
    circuit_breaker: CircuitBreaker,
}

/// Cached OAuth2 token with expiry.
#[derive(Debug, Clone)]
struct CachedToken {
    /// The access token
    token: String,
    /// When the token expires
    expires_at: Instant,
}

impl RestCatalogClient {
    /// Create a new REST catalog client.
    pub async fn new(config: &IcebergConfig, rest_uri: String) -> Result<Self> {
        let timeout = Duration::from_secs(config.rest.request_timeout_seconds.unwrap_or(30));

        let http_client = Client::builder()
            .timeout(timeout)
            .pool_max_idle_per_host(config.catalog_manager.connection_pool_size)
            .build()
            .map_err(|e| Error::Config(format!("Failed to create HTTP client: {}", e)))?;

        let client_config = RestClientConfig {
            base_uri: rest_uri.trim_end_matches('/').to_string(),
            timeout,
            max_retries: config.catalog_manager.max_retries,
            credential_type: config.rest.credential_type.clone(),
            bearer_token: config.rest.credential.clone(),
            oauth2_client_id: config.rest.oauth2_client_id.clone(),
            oauth2_client_secret: config.rest.oauth2_client_secret.clone(),
            oauth2_scope: config.rest.oauth2_scope.clone(),
            warehouse_path: config.warehouse_path.clone(),
        };

        // Create circuit breaker for fault tolerance
        let circuit_breaker = CircuitBreaker::new(
            CircuitBreakerConfig::new("rest-catalog")
                .with_failure_threshold(5)
                .with_reset_timeout(Duration::from_secs(30))
                .with_success_threshold(2),
        );

        let client = Self {
            client: http_client,
            config: client_config,
            oauth_token: RwLock::new(None),
            last_health_check: RwLock::new(None),
            circuit_breaker,
        };

        // Perform initial token fetch if using OAuth2
        if config.rest.credential_type == CredentialType::OAuth2 {
            client.refresh_oauth_token().await?;
        }

        info!(uri = %rest_uri, "REST catalog client initialized");
        Ok(client)
    }

    /// Get authorization header value.
    async fn get_auth_header(&self) -> Result<Option<String>> {
        match self.config.credential_type {
            CredentialType::None => Ok(None),
            CredentialType::Bearer => Ok(self
                .config
                .bearer_token
                .as_ref()
                .map(|t| format!("Bearer {}", t))),
            CredentialType::OAuth2 => {
                // Check if we have a valid cached token
                let needs_refresh = {
                    let token = self.oauth_token.read();
                    match token.as_ref() {
                        Some(cached) => Instant::now() >= cached.expires_at,
                        None => true,
                    }
                };

                if needs_refresh {
                    self.refresh_oauth_token().await?;
                }

                let token = self.oauth_token.read();
                Ok(token.as_ref().map(|t| format!("Bearer {}", t.token)))
            }
        }
    }

    /// Refresh OAuth2 token.
    async fn refresh_oauth_token(&self) -> Result<()> {
        let client_id = self
            .config
            .oauth2_client_id
            .as_ref()
            .ok_or_else(|| Error::Config("OAuth2 requires client_id".into()))?;
        let client_secret = self
            .config
            .oauth2_client_secret
            .as_ref()
            .ok_or_else(|| Error::Config("OAuth2 requires client_secret".into()))?;

        let _request = rest_api::OAuthTokenRequest {
            grant_type: "client_credentials".to_string(),
            client_id: Some(client_id.clone()),
            client_secret: Some(client_secret.clone()),
            scope: self.config.oauth2_scope.clone(),
        };

        let url = format!("{}/v1/oauth/tokens", self.config.base_uri);
        let response = self
            .client
            .post(&url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", client_id),
                ("client_secret", client_secret),
                ("scope", self.config.oauth2_scope.as_deref().unwrap_or("")),
            ])
            .send()
            .await
            .map_err(|e| Error::Iceberg(IcebergError::CatalogConnection(e.to_string())))?;

        if !response.status().is_success() {
            return Err(Error::Iceberg(IcebergError::CatalogConnection(format!(
                "OAuth2 token request failed: {}",
                response.status()
            ))));
        }

        let token_response: rest_api::OAuthTokenResponse = response
            .json()
            .await
            .map_err(|e| Error::Iceberg(IcebergError::CatalogConnection(e.to_string())))?;

        // Cache the token with expiry
        let expires_in = token_response.expires_in.unwrap_or(3600);
        let expires_at = Instant::now() + Duration::from_secs((expires_in - 60) as u64); // Refresh 60s early

        *self.oauth_token.write() = Some(CachedToken {
            token: token_response.access_token,
            expires_at,
        });

        debug!("OAuth2 token refreshed, expires in {}s", expires_in);
        Ok(())
    }

    /// Build a request with authentication.
    async fn build_request(
        &self,
        method: reqwest::Method,
        path: &str,
    ) -> Result<reqwest::RequestBuilder> {
        let url = format!("{}{}", self.config.base_uri, path);
        let mut request = self.client.request(method, &url);

        if let Some(auth) = self.get_auth_header().await? {
            request = request.header("Authorization", auth);
        }

        request = request.header("Content-Type", "application/json");
        request = request.header("Accept", "application/json");

        Ok(request)
    }

    /// Check if circuit breaker allows the request.
    fn check_circuit_breaker(&self) -> Result<()> {
        if !self.circuit_breaker.is_allowed() {
            warn!("Circuit breaker is open, rejecting request");
            return Err(Error::Iceberg(IcebergError::CatalogConnection(
                "Circuit breaker is open - catalog operations temporarily blocked".to_string(),
            )));
        }
        Ok(())
    }

    /// Execute a request with circuit breaker protection.
    async fn execute_request(&self, request: reqwest::RequestBuilder) -> Result<reqwest::Response> {
        self.check_circuit_breaker()?;

        match request.send().await {
            Ok(response) => {
                if response.status().is_success() || response.status().is_client_error() {
                    // Success or client error (4xx) - record success (client errors are not retryable)
                    self.circuit_breaker.record_success();
                } else {
                    // Server error (5xx) - record failure
                    self.circuit_breaker.record_failure();
                }
                Ok(response)
            }
            Err(e) => {
                // Connection error - record failure
                self.circuit_breaker.record_failure();
                Err(Error::Iceberg(IcebergError::CatalogConnection(
                    e.to_string(),
                )))
            }
        }
    }

    /// Handle error response from REST API.
    fn handle_error_response(&self, status: StatusCode, body: &str) -> Error {
        // Try to parse as ErrorResponse
        if let Ok(error_response) = serde_json::from_str::<rest_api::ErrorResponse>(body) {
            match status {
                StatusCode::NOT_FOUND => {
                    if error_response.error_type.contains("NoSuchTable") {
                        Error::Iceberg(IcebergError::TableNotFound(error_response.message))
                    } else if error_response.error_type.contains("NoSuchNamespace") {
                        Error::Iceberg(IcebergError::CatalogConnection(format!(
                            "Namespace not found: {}",
                            error_response.message
                        )))
                    } else {
                        Error::Iceberg(IcebergError::Other(error_response.message))
                    }
                }
                StatusCode::CONFLICT => {
                    // CAS conflict
                    Error::Iceberg(IcebergError::CasConflict {
                        expected: -1, // We don't have the exact values from the response
                        actual: -1,
                    })
                }
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                    Error::Iceberg(IcebergError::CatalogConnection(format!(
                        "Authentication failed: {}",
                        error_response.message
                    )))
                }
                _ => Error::Iceberg(IcebergError::Other(format!(
                    "{}: {}",
                    status, error_response.message
                ))),
            }
        } else {
            Error::Iceberg(IcebergError::Other(format!("{}: {}", status, body)))
        }
    }

    /// Convert API schema to our TableSchema.
    fn convert_schema(&self, schema: &rest_api::Schema) -> TableSchema {
        TableSchema {
            schema_id: schema.schema_id,
            fields: schema
                .fields
                .iter()
                .map(|f| {
                    let field_type = match &f.field_type {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    SchemaFieldInfo {
                        id: f.id,
                        name: f.name.clone(),
                        field_type,
                        required: f.required,
                        doc: f.doc.clone(),
                    }
                })
                .collect(),
        }
    }

    /// Convert our TableSchema to API schema.
    fn to_api_schema(&self, schema: &TableSchema) -> rest_api::Schema {
        rest_api::Schema {
            schema_id: schema.schema_id,
            r#type: "struct".to_string(),
            fields: schema
                .fields
                .iter()
                .map(|f| rest_api::SchemaField {
                    id: f.id,
                    name: f.name.clone(),
                    field_type: serde_json::Value::String(f.field_type.clone()),
                    required: f.required,
                    doc: f.doc.clone(),
                })
                .collect(),
            identifier_field_ids: vec![],
        }
    }
}

#[async_trait]
impl CatalogOperations for RestCatalogClient {
    async fn health_check(&self) -> Result<CatalogHealth> {
        let start = Instant::now();

        // Try to get config endpoint which is the standard health check
        let request = self
            .build_request(reqwest::Method::GET, "/v1/config")
            .await?;

        match request.send().await {
            Ok(response) => {
                let response_time_ms = start.elapsed().as_millis() as u64;
                let is_healthy = response.status().is_success();

                let message = if is_healthy {
                    Some(format!(
                        "REST catalog at {} is healthy",
                        self.config.base_uri
                    ))
                } else {
                    Some(format!(
                        "REST catalog returned status: {}",
                        response.status()
                    ))
                };

                let health = CatalogHealth {
                    is_healthy,
                    response_time_ms,
                    message,
                    catalog_type: CatalogType::Rest,
                };

                *self.last_health_check.write() = Some(health.clone());
                Ok(health)
            }
            Err(e) => {
                let response_time_ms = start.elapsed().as_millis() as u64;
                let health = CatalogHealth {
                    is_healthy: false,
                    response_time_ms,
                    message: Some(format!("Connection failed: {}", e)),
                    catalog_type: CatalogType::Rest,
                };

                *self.last_health_check.write() = Some(health.clone());
                Ok(health)
            }
        }
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let request = self
            .build_request(reqwest::Method::GET, "/v1/namespaces")
            .await?;
        let response = self.execute_request(request).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.handle_error_response(status, &body));
        }

        let list: rest_api::ListNamespacesResponse = response
            .json()
            .await
            .map_err(|e| Error::Iceberg(IcebergError::Other(e.to_string())))?;

        // Flatten namespace parts into dot-separated strings
        Ok(list.namespaces.into_iter().map(|ns| ns.join(".")).collect())
    }

    async fn namespace_exists(&self, namespace: &str) -> Result<bool> {
        let encoded_ns = urlencoding::encode(namespace);
        let path = format!("/v1/namespaces/{}", encoded_ns);
        let request = self.build_request(reqwest::Method::GET, &path).await?;

        let response = self.execute_request(request).await?;

        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(self.handle_error_response(status, &body))
            }
        }
    }

    async fn create_namespace(&self, namespace: &str) -> Result<()> {
        let request_body = rest_api::CreateNamespaceRequest {
            namespace: namespace.split('.').map(|s| s.to_string()).collect(),
            properties: std::collections::HashMap::new(),
        };

        let request = self
            .build_request(reqwest::Method::POST, "/v1/namespaces")
            .await?;
        let response = self.execute_request(request.json(&request_body)).await?;

        if !response.status().is_success() {
            let status = response.status();
            // Ignore "already exists" errors
            if status == StatusCode::CONFLICT {
                debug!(namespace = %namespace, "Namespace already exists");
                return Ok(());
            }
            let body = response.text().await.unwrap_or_default();
            return Err(self.handle_error_response(status, &body));
        }

        info!(namespace = %namespace, "Created namespace");
        Ok(())
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let encoded_ns = urlencoding::encode(namespace);
        let path = format!("/v1/namespaces/{}/tables", encoded_ns);
        let request = self.build_request(reqwest::Method::GET, &path).await?;

        let response = self.execute_request(request).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.handle_error_response(status, &body));
        }

        let list: rest_api::ListTablesResponse = response
            .json()
            .await
            .map_err(|e| Error::Iceberg(IcebergError::Other(e.to_string())))?;

        Ok(list.identifiers.into_iter().map(|id| id.name).collect())
    }

    async fn table_exists(&self, namespace: &str, table: &str) -> Result<bool> {
        let encoded_ns = urlencoding::encode(namespace);
        let encoded_table = urlencoding::encode(table);
        let path = format!("/v1/namespaces/{}/tables/{}", encoded_ns, encoded_table);
        let request = self.build_request(reqwest::Method::HEAD, &path).await?;

        let response = self.execute_request(request).await?;

        match response.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(self.handle_error_response(status, &body))
            }
        }
    }

    async fn load_table(&self, namespace: &str, table: &str) -> Result<TableInfo> {
        let encoded_ns = urlencoding::encode(namespace);
        let encoded_table = urlencoding::encode(table);
        let path = format!("/v1/namespaces/{}/tables/{}", encoded_ns, encoded_table);
        let request = self.build_request(reqwest::Method::GET, &path).await?;

        let response = self.execute_request(request).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.handle_error_response(status, &body));
        }

        let table_response: rest_api::LoadTableResponse = response
            .json()
            .await
            .map_err(|e| Error::Iceberg(IcebergError::Other(e.to_string())))?;

        Ok(TableInfo {
            namespace: namespace.to_string(),
            name: table.to_string(),
            location: table_response.metadata.location,
            current_snapshot_id: table_response.metadata.current_snapshot_id,
            schema: self.convert_schema(&table_response.metadata.schema),
            properties: table_response.metadata.properties,
        })
    }

    async fn create_table(
        &self,
        namespace: &str,
        table: &str,
        schema: &TableSchema,
    ) -> Result<TableInfo> {
        let encoded_ns = urlencoding::encode(namespace);
        let path = format!("/v1/namespaces/{}/tables", encoded_ns);

        let request_body = rest_api::CreateTableRequest {
            name: table.to_string(),
            location: None, // Let catalog assign
            schema: self.to_api_schema(schema),
            partition_spec: None,
            write_order: None,
            stage_create: None,
            properties: std::collections::HashMap::new(),
        };

        let request = self.build_request(reqwest::Method::POST, &path).await?;
        let response = self.execute_request(request.json(&request_body)).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.handle_error_response(status, &body));
        }

        let table_response: rest_api::LoadTableResponse = response
            .json()
            .await
            .map_err(|e| Error::Iceberg(IcebergError::Other(e.to_string())))?;

        info!(namespace = %namespace, table = %table, "Created table");

        Ok(TableInfo {
            namespace: namespace.to_string(),
            name: table.to_string(),
            location: table_response.metadata.location,
            current_snapshot_id: table_response.metadata.current_snapshot_id,
            schema: self.convert_schema(&table_response.metadata.schema),
            properties: table_response.metadata.properties,
        })
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
        let encoded_ns = urlencoding::encode(namespace);
        let encoded_table = urlencoding::encode(table);
        let path = format!("/v1/namespaces/{}/tables/{}", encoded_ns, encoded_table);

        // Generate new snapshot ID
        let new_snapshot_id = chrono::Utc::now().timestamp_millis();
        let timestamp_ms = chrono::Utc::now().timestamp_millis();

        // Build requirements for CAS
        let mut requirements = Vec::new();
        if let Some(expected_id) = commit.expected_snapshot_id {
            requirements.push(rest_api::TableRequirement::AssertRefSnapshotId {
                ref_name: "main".to_string(),
                snapshot_id: Some(expected_id),
            });
        }

        // Build updates
        let mut updates = Vec::new();

        // Create snapshot for added files
        if !commit.files_to_add.is_empty() {
            // Note: In a real implementation, we would need to:
            // 1. Write manifest files for the data files
            // 2. Write manifest list
            // 3. Reference the manifest list in the snapshot
            // For now, we simulate this
            let manifest_list_path = format!(
                "{}/{}/{}/metadata/snap-{}-{}.avro",
                self.config.warehouse_path,
                namespace,
                table,
                new_snapshot_id,
                uuid::Uuid::new_v4()
            );

            let snapshot = rest_api::Snapshot {
                snapshot_id: new_snapshot_id,
                parent_snapshot_id: commit.expected_snapshot_id,
                sequence_number: 1, // Would need to track this properly
                timestamp_ms,
                manifest_list: manifest_list_path,
                summary: commit.summary.clone(),
                schema_id: Some(0),
            };

            updates.push(rest_api::TableUpdate::AddSnapshot { snapshot });
            updates.push(rest_api::TableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                snapshot_id: new_snapshot_id,
                ref_type: "branch".to_string(),
                max_ref_age_ms: None,
                max_snapshot_age_ms: None,
                min_snapshots_to_keep: None,
            });
        }

        let request_body = rest_api::CommitTableRequest {
            identifier: None,
            requirements,
            updates,
        };

        let request = self.build_request(reqwest::Method::POST, &path).await?;
        let response = self.execute_request(request.json(&request_body)).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();

            // Handle CAS conflict specifically
            if status == StatusCode::CONFLICT {
                return Err(Error::Iceberg(IcebergError::CasConflict {
                    expected: commit.expected_snapshot_id.unwrap_or(-1),
                    actual: -1, // We don't know the actual from the response
                }));
            }

            return Err(self.handle_error_response(status, &body));
        }

        let commit_response: rest_api::CommitTableResponse = response
            .json()
            .await
            .map_err(|e| Error::Iceberg(IcebergError::Other(e.to_string())))?;

        let actual_snapshot_id = commit_response
            .metadata
            .current_snapshot_id
            .unwrap_or(new_snapshot_id);

        info!(
            namespace = %namespace,
            table = %table,
            snapshot_id = actual_snapshot_id,
            files_added = commit.files_to_add.len(),
            "Snapshot committed"
        );

        Ok(SnapshotCommitResult {
            snapshot_id: actual_snapshot_id,
            committed_at: chrono::Utc::now(),
            files_added: commit.files_to_add.len(),
            files_removed: commit.files_to_remove.len(),
        })
    }

    fn catalog_type(&self) -> CatalogType {
        CatalogType::Rest
    }

    fn warehouse_path(&self) -> &str {
        &self.config.warehouse_path
    }

    async fn close(&self) -> Result<()> {
        // Clear cached token
        *self.oauth_token.write() = None;
        info!("REST catalog client closed");
        Ok(())
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

    #[tokio::test]
    async fn test_rest_catalog_factory_requires_uri() {
        let factory = RestCatalogFactory;
        let config = IcebergConfig {
            catalog_type: CatalogType::Rest,
            warehouse_path: "s3://bucket/warehouse".into(),
            database_name: "db".into(),
            table_name: "table".into(),
            target_file_size_mb: 512,
            compression: crate::config::ParquetCompression::Snappy,
            partition_spec: vec![],
            rest_uri: None, // Missing!
            hive_metastore_uri: None,
            aws_region: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            s3_endpoint: None,
            catalog_manager: Default::default(),
            table_management: Default::default(),
            rest: Default::default(),
            glue: Default::default(),
            nessie: None,
        };

        let result = factory.create(&config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rest_catalog_factory_creates_client() {
        let factory = RestCatalogFactory;
        let config = IcebergConfig {
            catalog_type: CatalogType::Rest,
            warehouse_path: "s3://bucket/warehouse".into(),
            database_name: "db".into(),
            table_name: "table".into(),
            target_file_size_mb: 512,
            compression: crate::config::ParquetCompression::Snappy,
            partition_spec: vec![],
            rest_uri: Some("http://localhost:8181".into()),
            hive_metastore_uri: None,
            aws_region: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            s3_endpoint: None,
            catalog_manager: Default::default(),
            table_management: Default::default(),
            rest: Default::default(),
            glue: Default::default(),
            nessie: None,
        };

        let result = factory.create(&config).await;
        assert!(result.is_ok());

        let catalog = result.unwrap();
        assert_eq!(catalog.catalog_type(), CatalogType::Rest);
        assert_eq!(catalog.warehouse_path(), "s3://bucket/warehouse");
    }

    #[tokio::test]
    async fn test_rest_catalog_health_check() {
        let config = IcebergConfig {
            catalog_type: CatalogType::Rest,
            warehouse_path: "s3://bucket/warehouse".into(),
            database_name: "db".into(),
            table_name: "table".into(),
            target_file_size_mb: 512,
            compression: crate::config::ParquetCompression::Snappy,
            partition_spec: vec![],
            rest_uri: Some("http://localhost:8181".into()),
            hive_metastore_uri: None,
            aws_region: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            s3_endpoint: None,
            catalog_manager: Default::default(),
            table_management: Default::default(),
            rest: Default::default(),
            glue: Default::default(),
            nessie: None,
        };

        // Create client - note this is now async
        let client = RestCatalogClient::new(&config, "http://localhost:8181".into())
            .await
            .unwrap();

        // Health check will fail as there's no server, but we can verify the client is created
        let health = client.health_check().await.unwrap();

        // Health check returns unhealthy when server is unreachable
        // but the check itself succeeds (doesn't error)
        assert_eq!(health.catalog_type, CatalogType::Rest);
    }

    #[test]
    fn test_data_file_info() {
        let file = DataFileInfo {
            file_path: "s3://bucket/data/file.parquet".into(),
            file_size_bytes: 1024 * 1024,
            record_count: 1000,
            partition_values: std::collections::HashMap::new(),
            file_format: "parquet".into(),
        };

        assert_eq!(file.file_path, "s3://bucket/data/file.parquet");
        assert_eq!(file.record_count, 1000);
    }

    #[test]
    fn test_snapshot_commit() {
        let commit = SnapshotCommit {
            expected_snapshot_id: Some(100),
            files_to_add: vec![DataFileInfo {
                file_path: "s3://bucket/data/file.parquet".into(),
                file_size_bytes: 1024,
                record_count: 100,
                partition_values: std::collections::HashMap::new(),
                file_format: "parquet".into(),
            }],
            files_to_remove: vec![],
            summary: std::collections::HashMap::new(),
        };

        assert_eq!(commit.expected_snapshot_id, Some(100));
        assert_eq!(commit.files_to_add.len(), 1);
    }
}
