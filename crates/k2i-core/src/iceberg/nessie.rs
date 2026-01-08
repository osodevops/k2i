//! Nessie catalog implementation.
//!
//! Nessie is a transactional catalog for Apache Iceberg that provides
//! Git-like versioned data lake management. It exposes the Iceberg REST Catalog API
//! with additional versioning capabilities.
//!
//! This implementation wraps the REST catalog client with Nessie-specific headers
//! and reference (branch/tag) support.

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::config::{CatalogType, CredentialType, IcebergConfig, NessieCatalogConfig};
use crate::iceberg::factory::{
    CatalogFactory, CatalogHealth, CatalogOperations, DataFileInfo, SchemaFieldInfo,
    SnapshotCommit, SnapshotCommitResult, TableInfo, TableSchema,
};
use crate::iceberg::rest_api;
use crate::{Error, IcebergError, Result};
use async_trait::async_trait;
use parking_lot::RwLock;
use reqwest::{Client, StatusCode};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Factory for Nessie catalog.
pub struct NessieCatalogFactory;

#[async_trait]
impl CatalogFactory for NessieCatalogFactory {
    async fn create(&self, config: &IcebergConfig) -> Result<Arc<dyn CatalogOperations>> {
        let rest_uri = config
            .rest_uri
            .as_ref()
            .ok_or_else(|| Error::Config("Nessie catalog requires rest_uri to be set".into()))?;

        let client = NessieCatalogClient::new(config, rest_uri.clone()).await?;
        Ok(Arc::new(client))
    }

    fn catalog_type(&self) -> CatalogType {
        CatalogType::Nessie
    }
}

/// Nessie catalog client configuration.
#[derive(Debug, Clone)]
pub struct NessieClientConfig {
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
    /// Warehouse path
    pub warehouse_path: String,
    /// Default reference (branch or tag)
    pub default_reference: String,
    /// API version (v1 or v2)
    pub api_version: String,
}

/// Nessie catalog client implementation.
///
/// Implements the Apache Iceberg REST Catalog API with Nessie-specific headers.
pub struct NessieCatalogClient {
    /// HTTP client
    client: Client,
    /// Client configuration
    config: NessieClientConfig,
    /// Cached OAuth2 token (if using OAuth2 auth)
    oauth_token: RwLock<Option<CachedToken>>,
    /// Last health check result
    last_health_check: RwLock<Option<CatalogHealth>>,
    /// Circuit breaker for fault tolerance
    circuit_breaker: CircuitBreaker,
    /// Current reference being used
    current_reference: RwLock<String>,
}

/// Cached OAuth2 token with expiry.
#[derive(Debug, Clone)]
struct CachedToken {
    /// The access token
    token: String,
    /// When the token expires
    expires_at: Instant,
}

impl NessieCatalogClient {
    /// Create a new Nessie catalog client.
    pub async fn new(config: &IcebergConfig, rest_uri: String) -> Result<Self> {
        let timeout = Duration::from_secs(config.rest.request_timeout_seconds.unwrap_or(30) as u64);

        let http_client = Client::builder()
            .timeout(timeout)
            .pool_max_idle_per_host(config.catalog_manager.connection_pool_size as usize)
            .build()
            .map_err(|e| Error::Config(format!("Failed to create HTTP client: {}", e)))?;

        // Get Nessie-specific config
        let nessie_config = config.nessie.clone().unwrap_or_default();
        let default_ref = nessie_config
            .default_branch
            .unwrap_or_else(|| "main".to_string());
        let api_version = nessie_config
            .api_version
            .unwrap_or_else(|| "v1".to_string());

        let client_config = NessieClientConfig {
            base_uri: rest_uri.trim_end_matches('/').to_string(),
            timeout,
            max_retries: config.catalog_manager.max_retries,
            credential_type: config.rest.credential_type.clone(),
            bearer_token: config.rest.credential.clone(),
            warehouse_path: config.warehouse_path.clone(),
            default_reference: default_ref.clone(),
            api_version,
        };

        // Create circuit breaker for fault tolerance
        let circuit_breaker = CircuitBreaker::new(
            CircuitBreakerConfig::new("nessie-catalog")
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
            current_reference: RwLock::new(default_ref),
        };

        info!(
            uri = %rest_uri,
            default_ref = %client.current_reference.read(),
            "Nessie catalog client initialized"
        );

        Ok(client)
    }

    /// Get the current reference (branch/tag).
    pub fn current_reference(&self) -> String {
        self.current_reference.read().clone()
    }

    /// Set the current reference (branch/tag).
    pub fn set_reference(&self, reference: impl Into<String>) {
        let new_ref = reference.into();
        info!(reference = %new_ref, "Switching Nessie reference");
        *self.current_reference.write() = new_ref;
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
                    // For Nessie, OAuth2 token refresh would go here
                    // This is similar to REST catalog but may use Nessie-specific endpoints
                    warn!("OAuth2 token refresh not yet implemented for Nessie");
                }

                let token = self.oauth_token.read();
                Ok(token.as_ref().map(|t| format!("Bearer {}", t.token)))
            }
        }
    }

    /// Build a request with authentication and Nessie-specific headers.
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

        // Standard headers
        request = request.header("Content-Type", "application/json");
        request = request.header("Accept", "application/json");

        // Nessie-specific headers
        let current_ref = self.current_reference.read().clone();
        request = request.header("Nessie-Reference", &current_ref);
        request = request.header("Nessie-API-Version", &self.config.api_version);

        Ok(request)
    }

    /// Check if circuit breaker allows the request.
    fn check_circuit_breaker(&self) -> Result<()> {
        if !self.circuit_breaker.is_allowed() {
            warn!("Circuit breaker is open, rejecting request");
            return Err(Error::Iceberg(IcebergError::CatalogConnection(
                "Circuit breaker is open - Nessie operations temporarily blocked".to_string(),
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
                    self.circuit_breaker.record_success();
                } else {
                    self.circuit_breaker.record_failure();
                }
                Ok(response)
            }
            Err(e) => {
                self.circuit_breaker.record_failure();
                Err(Error::Iceberg(IcebergError::CatalogConnection(
                    e.to_string(),
                )))
            }
        }
    }

    /// Handle error response from REST API.
    fn handle_error_response(&self, status: StatusCode, body: &str) -> Error {
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
                    } else if error_response.error_type.contains("NessieRefNotFound") {
                        Error::Iceberg(IcebergError::CatalogConnection(format!(
                            "Nessie reference not found: {}",
                            error_response.message
                        )))
                    } else {
                        Error::Iceberg(IcebergError::Other(error_response.message))
                    }
                }
                StatusCode::CONFLICT => Error::Iceberg(IcebergError::CasConflict {
                    expected: -1,
                    actual: -1,
                }),
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
impl CatalogOperations for NessieCatalogClient {
    async fn health_check(&self) -> Result<CatalogHealth> {
        let start = Instant::now();

        // Nessie health check - try to get config or reference info
        let request = self
            .build_request(reqwest::Method::GET, "/v1/config")
            .await?;

        match request.send().await {
            Ok(response) => {
                let response_time_ms = start.elapsed().as_millis() as u64;
                let is_healthy = response.status().is_success();

                let message = if is_healthy {
                    Some(format!(
                        "Nessie catalog at {} is healthy (ref: {})",
                        self.config.base_uri,
                        self.current_reference()
                    ))
                } else {
                    Some(format!(
                        "Nessie catalog returned status: {}",
                        response.status()
                    ))
                };

                let health = CatalogHealth {
                    is_healthy,
                    response_time_ms,
                    message,
                    catalog_type: CatalogType::Nessie,
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
                    catalog_type: CatalogType::Nessie,
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
            if status == StatusCode::CONFLICT {
                debug!(namespace = %namespace, "Namespace already exists");
                return Ok(());
            }
            let body = response.text().await.unwrap_or_default();
            return Err(self.handle_error_response(status, &body));
        }

        info!(
            namespace = %namespace,
            reference = %self.current_reference(),
            "Created namespace in Nessie"
        );
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
            location: None,
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

        info!(
            namespace = %namespace,
            table = %table,
            reference = %self.current_reference(),
            "Created table in Nessie"
        );

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

        let new_snapshot_id = chrono::Utc::now().timestamp_millis();
        let timestamp_ms = chrono::Utc::now().timestamp_millis();

        let mut requirements = Vec::new();
        if let Some(expected_id) = commit.expected_snapshot_id {
            requirements.push(rest_api::TableRequirement::AssertRefSnapshotId {
                ref_name: "main".to_string(),
                snapshot_id: Some(expected_id),
            });
        }

        let mut updates = Vec::new();

        if !commit.files_to_add.is_empty() {
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
                sequence_number: 1,
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

            if status == StatusCode::CONFLICT {
                return Err(Error::Iceberg(IcebergError::CasConflict {
                    expected: commit.expected_snapshot_id.unwrap_or(-1),
                    actual: -1,
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
            reference = %self.current_reference(),
            files_added = commit.files_to_add.len(),
            "Snapshot committed to Nessie"
        );

        Ok(SnapshotCommitResult {
            snapshot_id: actual_snapshot_id,
            committed_at: chrono::Utc::now(),
            files_added: commit.files_to_add.len(),
            files_removed: commit.files_to_remove.len(),
        })
    }

    fn catalog_type(&self) -> CatalogType {
        CatalogType::Nessie
    }

    fn warehouse_path(&self) -> &str {
        &self.config.warehouse_path
    }

    async fn close(&self) -> Result<()> {
        *self.oauth_token.write() = None;
        info!(
            reference = %self.current_reference(),
            "Nessie catalog client closed"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ParquetCompression;

    fn create_test_config() -> IcebergConfig {
        IcebergConfig {
            catalog_type: CatalogType::Nessie,
            warehouse_path: "s3://bucket/warehouse".into(),
            database_name: "db".into(),
            table_name: "table".into(),
            target_file_size_mb: 512,
            compression: ParquetCompression::Snappy,
            partition_spec: vec![],
            rest_uri: Some("http://localhost:19120/api/v1".into()),
            hive_metastore_uri: None,
            aws_region: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            s3_endpoint: None,
            catalog_manager: Default::default(),
            table_management: Default::default(),
            rest: Default::default(),
            glue: Default::default(),
            nessie: Some(NessieCatalogConfig {
                default_branch: Some("main".into()),
                api_version: Some("v1".into()),
            }),
        }
    }

    #[test]
    fn test_nessie_catalog_factory_type() {
        let factory = NessieCatalogFactory;
        assert_eq!(factory.catalog_type(), CatalogType::Nessie);
    }

    #[tokio::test]
    async fn test_nessie_catalog_factory_requires_uri() {
        let factory = NessieCatalogFactory;
        let mut config = create_test_config();
        config.rest_uri = None;

        let result = factory.create(&config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nessie_catalog_factory_creates_client() {
        let factory = NessieCatalogFactory;
        let config = create_test_config();

        let result = factory.create(&config).await;
        assert!(result.is_ok());

        let catalog = result.unwrap();
        assert_eq!(catalog.catalog_type(), CatalogType::Nessie);
        assert_eq!(catalog.warehouse_path(), "s3://bucket/warehouse");
    }

    #[tokio::test]
    async fn test_nessie_catalog_health_check() {
        let config = create_test_config();
        let client = NessieCatalogClient::new(&config, "http://localhost:19120/api/v1".into())
            .await
            .unwrap();

        let health = client.health_check().await.unwrap();
        assert_eq!(health.catalog_type, CatalogType::Nessie);
        // Health check will fail without a running Nessie server
    }

    #[tokio::test]
    async fn test_nessie_reference_switching() {
        let config = create_test_config();
        let client = NessieCatalogClient::new(&config, "http://localhost:19120/api/v1".into())
            .await
            .unwrap();

        assert_eq!(client.current_reference(), "main");

        client.set_reference("develop");
        assert_eq!(client.current_reference(), "develop");

        client.set_reference("feature/test");
        assert_eq!(client.current_reference(), "feature/test");
    }
}
