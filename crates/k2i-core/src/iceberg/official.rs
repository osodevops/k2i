//! Official Iceberg metadata commit support.
//!
//! This module uses Apache iceberg-rust transaction APIs to produce real
//! metadata JSON, manifest lists, and manifest files. The existing
//! `CatalogOperations` implementations remain useful for lifecycle and health
//! checks, but append commits that need DuckDB `iceberg_scan` compatibility
//! should go through this path.
//!
//! ## Adapter over the official REST catalog client
//!
//! [`OfficialRestCommitter`] now also implements [`CatalogOperations`] by
//! delegating most methods to the underlying `iceberg_catalog_rest::RestCatalog`.
//! The one exception is `update_schema` — the official client's
//! `Transaction::update_schema()` is merged to `apache/iceberg-rust` `main`
//! but is not available in K2I's currently pinned `iceberg` release.
//!
//! Until K2I upgrades to an `iceberg` release containing that API,
//! `update_schema` is handled by a small, standalone REST codepath in this
//! file. It is explicitly temporary scaffolding tracked for removal by the
//! follow-up feature `003-iceberg-010-readiness-spike`.

use crate::config::{CatalogType, CredentialType, IcebergConfig};
use crate::iceberg::factory::{
    CatalogHealth, CatalogOperations, DataFileInfo, SchemaFieldInfo, SnapshotCommit,
    SnapshotCommitResult, TableInfo, TableSchema,
};
use crate::iceberg::rest_api;
use crate::{Error, IcebergError, Result};
use ::iceberg::{Catalog, CatalogBuilder};
use async_trait::async_trait;
use iceberg_catalog_rest::{
    RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use parking_lot::{Mutex, RwLock};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Client as HttpClient;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as AsyncMutex, OnceCell};
use tracing::{debug, info};

const RECENT_MANIFEST_LIST_LIMIT: usize = 64;

/// Result of a real Iceberg append commit.
#[derive(Debug, Clone)]
pub struct OfficialCommitResult {
    /// Committed snapshot ID from the Iceberg table metadata.
    pub snapshot_id: i64,
    /// Real manifest-list path for the committed snapshot.
    pub manifest_list_path: String,
}

/// OAuth2 token cached in memory for the standalone `update_schema` path.
#[derive(Debug, Clone)]
struct CachedOAuthToken {
    token: String,
    expires_at: Instant,
}

/// Runtime route negotiated through the REST catalog `/v1/config` endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedRestRoute {
    base_uri: String,
    prefix: Option<String>,
}

/// REST-catalog backed official Iceberg committer.
///
/// Most [`CatalogOperations`] methods delegate to the underlying `RestCatalog`.
/// `update_schema` is handled by a standalone REST codepath (see module docs).
pub struct OfficialRestCommitter {
    catalog: RestCatalog,
    warehouse_path: String,
    // ---- fields for the standalone update_schema path ----
    rest_uri: String,
    warehouse_name: String,
    http_client: HttpClient,
    /// Cached runtime route, resolved exactly once from `/v1/config`.
    ///
    /// `tokio::sync::OnceCell::get_or_try_init` makes concurrent first callers
    /// share one request. Failed resolutions are not cached, so a transient
    /// failure does not poison later attempts.
    route_cache: OnceCell<ResolvedRestRoute>,
    credential_type: CredentialType,
    bearer_token: Option<String>,
    oauth2_client_id: Option<String>,
    oauth2_client_secret: Option<String>,
    oauth2_scope: Option<String>,
    oauth2_token_endpoint: Option<String>,
    /// Cached OAuth2 access token for the standalone path.
    oauth2_token: RwLock<Option<CachedOAuthToken>>,
    /// Prevent concurrent requests from stampeding the OAuth2 token endpoint.
    oauth2_refresh_lock: AsyncMutex<()>,
    /// Bounded commit metadata cache used to preserve the real manifest-list
    /// path through the catalog abstraction without changing its result type.
    recent_manifest_lists: Mutex<VecDeque<(i64, String)>>,
}

impl OfficialRestCommitter {
    /// Returns true when this config should use the official REST commit path.
    pub fn is_enabled(config: &IcebergConfig) -> bool {
        config.catalog_type == CatalogType::Rest && config.rest_uri.is_some()
    }

    /// Build a committer from K2I Iceberg config.
    pub async fn new(config: &IcebergConfig) -> Result<Self> {
        let rest_uri = config
            .rest_uri
            .as_ref()
            .ok_or_else(|| Error::Config("official REST committer requires rest_uri".into()))?;

        let wh = warehouse_uri(&config.warehouse_path)?;

        let mut props = HashMap::from([
            (REST_CATALOG_PROP_URI.to_string(), rest_uri.clone()),
            (REST_CATALOG_PROP_WAREHOUSE.to_string(), wh.clone()),
        ]);

        apply_rest_auth_props(config, &mut props)?;
        apply_file_io_props(config, &mut props);

        for (key, value) in &config.rest.custom_headers {
            props.insert(format!("header.{}", key), value.clone());
        }

        let catalog = RestCatalogBuilder::default()
            .load("k2i", props)
            .await
            .map_err(map_iceberg_error)?;

        // Build an HTTP client for the standalone update_schema path.
        let timeout_secs = config.rest.request_timeout_seconds.unwrap_or(30);
        let http_client = HttpClient::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .pool_max_idle_per_host(config.catalog_manager.connection_pool_size)
            .default_headers(custom_header_map(&config.rest.custom_headers)?)
            .build()
            .map_err(|e| Error::Config(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            catalog,
            warehouse_path: config.warehouse_path.clone(),
            rest_uri: rest_uri.trim_end_matches('/').to_string(),
            warehouse_name: wh,
            http_client,
            route_cache: OnceCell::new(),
            credential_type: config.rest.credential_type.clone(),
            bearer_token: config.rest.credential.clone(),
            oauth2_client_id: config.rest.oauth2_client_id.clone(),
            oauth2_client_secret: config.rest.oauth2_client_secret.clone(),
            oauth2_scope: config.rest.oauth2_scope.clone(),
            oauth2_token_endpoint: config.rest.oauth2_token_endpoint.clone(),
            oauth2_token: RwLock::new(None),
            oauth2_refresh_lock: AsyncMutex::new(()),
            recent_manifest_lists: Mutex::new(VecDeque::new()),
        })
    }

    /// Commit existing Parquet files to an existing Iceberg table.
    pub async fn commit_append(
        &self,
        namespace: &str,
        table: &str,
        files: Vec<DataFileInfo>,
        summary: HashMap<String, String>,
    ) -> Result<OfficialCommitResult> {
        commit_append_with_catalog(
            &self.catalog,
            &self.warehouse_path,
            namespace,
            table,
            files,
            summary,
        )
        .await
    }

    // ---- Standalone update_schema helpers ----

    /// Return the auth header value for standalone REST calls.
    async fn get_auth_header(&self) -> Result<Option<String>> {
        match self.credential_type {
            CredentialType::None => Ok(None),
            CredentialType::Bearer => {
                Ok(self.bearer_token.as_ref().map(|t| format!("Bearer {}", t)))
            }
            CredentialType::OAuth2 => {
                let cached_header = {
                    let token = self.oauth2_token.read();
                    match token.as_ref() {
                        Some(cached) if Instant::now() < cached.expires_at => {
                            Some(format!("Bearer {}", cached.token))
                        }
                        _ => None,
                    }
                };
                if cached_header.is_some() {
                    return Ok(cached_header);
                }

                // Re-check after acquiring the refresh lock: another caller
                // may have refreshed the token while this task was waiting.
                let _refresh_guard = self.oauth2_refresh_lock.lock().await;
                let cached_header = {
                    let token = self.oauth2_token.read();
                    match token.as_ref() {
                        Some(cached) if Instant::now() < cached.expires_at => {
                            Some(format!("Bearer {}", cached.token))
                        }
                        _ => None,
                    }
                };
                if cached_header.is_some() {
                    return Ok(cached_header);
                }

                self.refresh_oauth2_token().await?;
                let token = self.oauth2_token.read();
                Ok(token.as_ref().map(|t| format!("Bearer {}", t.token)))
            }
        }
    }

    /// Refresh OAuth2 token via the token endpoint.
    async fn refresh_oauth2_token(&self) -> Result<()> {
        let client_id = self
            .oauth2_client_id
            .as_ref()
            .ok_or_else(|| Error::Config("OAuth2 requires client_id for standalone path".into()))?;
        let client_secret = self.oauth2_client_secret.as_ref().ok_or_else(|| {
            Error::Config("OAuth2 requires client_secret for standalone path".into())
        })?;

        let url = oauth2_token_url(&self.rest_uri, self.oauth2_token_endpoint.as_deref());
        let response = self
            .http_client
            .post(&url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", client_id),
                ("client_secret", client_secret),
                ("scope", self.oauth2_scope.as_deref().unwrap_or("catalog")),
            ])
            .send()
            .await
            .map_err(|e| {
                Error::Iceberg(IcebergError::CatalogConnection(format!(
                    "OAuth2 token request failed: {}",
                    e
                )))
            })?;

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

        let expires_in = token_response.expires_in.unwrap_or(3600);
        let ttl_seconds = u64::try_from(expires_in).map_err(|_| {
            Error::Iceberg(IcebergError::CatalogConnection(format!(
                "OAuth2 token response contained a negative expires_in value: {}",
                expires_in
            )))
        })?;
        let refresh_skew = (ttl_seconds / 10).clamp(1, 60);
        let expires_at =
            Instant::now() + Duration::from_secs(ttl_seconds.saturating_sub(refresh_skew));

        *self.oauth2_token.write() = Some(CachedOAuthToken {
            token: token_response.access_token,
            expires_at,
        });

        debug!(
            "OAuth2 token refreshed for standalone path, expires in {}s",
            expires_in
        );
        Ok(())
    }

    /// Resolve and cache the runtime REST route from `/v1/config`.
    async fn do_resolve_route(&self) -> Result<ResolvedRestRoute> {
        let route = self
            .route_cache
            .get_or_try_init(|| async {
                let url = format!("{}/v1/config", self.rest_uri);
                let mut req = self.http_client.get(&url);

                if !self.warehouse_name.is_empty() {
                    req = req.query(&[("warehouse", &self.warehouse_name)]);
                }

                if let Some(auth) = self.get_auth_header().await? {
                    req = req.header("Authorization", auth);
                }

                let response = req
                    .header("Accept", "application/json")
                    .send()
                    .await
                    .map_err(|e| {
                        Error::Iceberg(IcebergError::CatalogConnection(format!(
                            "Failed to fetch /v1/config: {}",
                            e
                        )))
                    })?;

                if !response.status().is_success() {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    return Err(Error::Iceberg(IcebergError::CatalogConnection(format!(
                        "/v1/config returned {}: {}",
                        status, body
                    ))));
                }

                let config: rest_api::CatalogConfig = response.json().await.map_err(|e| {
                    Error::Iceberg(IcebergError::CatalogConnection(format!(
                        "Failed to parse /v1/config response: {}",
                        e
                    )))
                })?;

                Ok(ResolvedRestRoute {
                    base_uri: extract_uri(&config)
                        .unwrap_or_else(|| self.rest_uri.clone())
                        .trim_end_matches('/')
                        .to_string(),
                    prefix: extract_prefix(&config),
                })
            })
            .await?;

        Ok(route.clone())
    }

    /// Build the request URL for the standalone update_schema call.
    fn update_schema_url(
        &self,
        route: &ResolvedRestRoute,
        namespace: &str,
        table: &str,
    ) -> Result<String> {
        build_update_schema_url(&route.base_uri, route.prefix.as_deref(), namespace, table)
    }

    /// Standalone `update_schema` implementation.
    ///
    /// This is a temporary, narrowly-scoped hand-rolled REST codepath that
    /// exists only because the official `iceberg-rust` client's
    /// `Transaction::update_schema()` is not yet available on crates.io
    /// (see module-level docs).
    ///
    /// TODO(iceberg-0.10): Delete this entire implementation once
    /// `iceberg-rust` 0.10.0+ is published to crates.io with a stable
    /// `Transaction::update_schema()` API.
    ///
    /// **Bug fixes vs. the deleted `RestCatalogClient`:**
    /// - Prefix is resolved **exactly once** and cached (no per-call re-resolution).
    /// - A transient error after the first successful resolution **never**
    ///   overwrites the cached prefix.
    /// - If the *first* resolution attempt fails, the method returns an error
    ///   (no silent fallback to unprefixed paths).
    async fn update_schema_standalone(
        &self,
        namespace: &str,
        table: &str,
        schema: &TableSchema,
        expected_schema_id: Option<i32>,
    ) -> Result<TableInfo> {
        // Resolve the runtime route once, including any catalog-provided URI
        // override and multi-warehouse prefix.
        let route = self.do_resolve_route().await?;
        let url = self.update_schema_url(&route, namespace, table)?;

        let mut requirements = Vec::new();
        if let Some(current_schema_id) = expected_schema_id {
            requirements
                .push(rest_api::TableRequirement::AssertCurrentSchemaId { current_schema_id });
        }

        let last_column_id = schema.fields.iter().map(|field| field.id).max();
        let updates = vec![
            rest_api::TableUpdate::AddSchema {
                schema: self.to_api_schema(schema),
                last_column_id,
            },
            rest_api::TableUpdate::SetCurrentSchema {
                schema_id: schema.schema_id,
            },
        ];

        let request_body = rest_api::CommitTableRequest {
            identifier: None,
            requirements,
            updates,
        };

        let mut req = self.http_client.post(&url).json(&request_body);
        req = req.header("Content-Type", "application/json");
        req = req.header("Accept", "application/json");

        if let Some(auth) = self.get_auth_header().await? {
            req = req.header("Authorization", auth);
        }

        let response = req.send().await.map_err(|e| {
            Error::Iceberg(IcebergError::CatalogConnection(format!(
                "update_schema request failed: {}",
                e
            )))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();

            if status == reqwest::StatusCode::CONFLICT {
                return Err(Error::Iceberg(IcebergError::SchemaEvolution(format!(
                    "schema update conflict for {}.{} while moving to schema ID {}",
                    namespace, table, schema.schema_id
                ))));
            }

            return Err(self.handle_error_response(status, &body));
        }

        let commit_response: rest_api::CommitTableResponse =
            response.json().await.map_err(|e| {
                Error::Iceberg(IcebergError::Other(format!(
                    "Failed to parse commit response: {}",
                    e
                )))
            })?;

        let committed_schema = self.convert_current_schema(&commit_response.metadata)?;

        info!(
            namespace = %namespace,
            table = %table,
            schema_id = schema.schema_id,
            field_count = schema.fields.len(),
            "Schema update committed via standalone REST path"
        );

        Ok(TableInfo {
            namespace: namespace.to_string(),
            name: table.to_string(),
            location: commit_response.metadata.location,
            current_snapshot_id: commit_response.metadata.current_snapshot_id,
            schema: committed_schema,
            properties: commit_response.metadata.properties,
        })
    }

    /// Convert a K2I `TableSchema` to a `rest_api::Schema`.
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
                    field_type: schema_type_to_api_value(&f.field_type),
                    required: f.required,
                    doc: f.doc.clone(),
                })
                .collect(),
            identifier_field_ids: vec![],
        }
    }

    /// Convert a `rest_api::TableMetadata` to a `TableSchema`.
    fn convert_current_schema(&self, metadata: &rest_api::TableMetadata) -> Result<TableSchema> {
        let schema = metadata.current_schema().ok_or_else(|| {
            Error::Iceberg(IcebergError::Other(format!(
                "REST catalog table metadata at {} has no current schema",
                metadata.location
            )))
        })?;
        Ok(self.convert_schema(schema))
    }

    /// Convert a `rest_api::Schema` to a `TableSchema`.
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

    /// Handle error response from REST API.
    fn handle_error_response(&self, status: reqwest::StatusCode, body: &str) -> Error {
        if let Ok(error_response) = serde_json::from_str::<rest_api::ErrorResponse>(body) {
            match status {
                reqwest::StatusCode::NOT_FOUND => {
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
                reqwest::StatusCode::CONFLICT => Error::Iceberg(IcebergError::CasConflict {
                    expected: -1,
                    actual: -1,
                }),
                reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
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

    fn remember_manifest_list(&self, result: &OfficialCommitResult) {
        let mut recent = self.recent_manifest_lists.lock();
        if let Some(index) = recent
            .iter()
            .position(|(snapshot_id, _)| *snapshot_id == result.snapshot_id)
        {
            recent.remove(index);
        }
        recent.push_back((result.snapshot_id, result.manifest_list_path.clone()));
        while recent.len() > RECENT_MANIFEST_LIST_LIMIT {
            recent.pop_front();
        }
    }
}

#[async_trait]
impl CatalogOperations for OfficialRestCommitter {
    async fn health_check(&self) -> Result<CatalogHealth> {
        let start = Instant::now();

        // Use list_namespaces as a health probe — this implicitly validates
        // prefix negotiation + auth in one round-trip.  The official client
        // caches prefix resolution internally via OnceCell, so a subsequent
        // health check never re-resolves /v1/config.
        match self.catalog.list_namespaces(None).await {
            Ok(_) => {
                let elapsed = start.elapsed().as_millis() as u64;
                Ok(CatalogHealth {
                    is_healthy: true,
                    response_time_ms: elapsed,
                    message: Some(format!("REST catalog at {} is healthy", self.rest_uri)),
                    catalog_type: CatalogType::Rest,
                })
            }
            Err(e) => {
                let elapsed = start.elapsed().as_millis() as u64;
                Ok(CatalogHealth {
                    is_healthy: false,
                    response_time_ms: elapsed,
                    message: Some(format!("Catalog health check failed: {}", e)),
                    catalog_type: CatalogType::Rest,
                })
            }
        }
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let namespaces = self
            .catalog
            .list_namespaces(None)
            .await
            .map_err(map_iceberg_error)?;
        Ok(namespaces
            .into_iter()
            .map(|ns| ns.inner().join("."))
            .collect())
    }

    async fn namespace_exists(&self, namespace: &str) -> Result<bool> {
        let ident = namespace_ident(namespace)?;
        self.catalog
            .namespace_exists(&ident)
            .await
            .map_err(map_iceberg_error)
    }

    async fn create_namespace(&self, namespace: &str) -> Result<()> {
        let ident = namespace_ident(namespace)?;
        match self.catalog.create_namespace(&ident, HashMap::new()).await {
            Ok(_) => {
                info!(namespace = %namespace, "Created namespace");
                Ok(())
            }
            Err(e) if e.kind() == ::iceberg::ErrorKind::NamespaceAlreadyExists => {
                debug!(namespace = %namespace, "Namespace already exists");
                Ok(())
            }
            Err(e) => Err(map_iceberg_error(e)),
        }
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let ident = namespace_ident(namespace)?;
        let tables = self
            .catalog
            .list_tables(&ident)
            .await
            .map_err(map_iceberg_error)?;
        Ok(tables.into_iter().map(|t| t.name).collect())
    }

    async fn table_exists(&self, namespace: &str, table: &str) -> Result<bool> {
        let ident = table_ident(namespace, table)?;
        self.catalog
            .table_exists(&ident)
            .await
            .map_err(map_iceberg_error)
    }

    async fn load_table(&self, namespace: &str, table: &str) -> Result<TableInfo> {
        let ident = table_ident(namespace, table)?;
        let loaded = self
            .catalog
            .load_table(&ident)
            .await
            .map_err(map_iceberg_error)?;
        table_info_from(namespace, table, &loaded)
    }

    async fn create_table(
        &self,
        namespace: &str,
        table: &str,
        schema: &TableSchema,
    ) -> Result<TableInfo> {
        let ns_ident = namespace_ident(namespace)?;
        let iceberg_schema = table_schema_to_iceberg_schema(schema)?;
        let table_creation = ::iceberg::TableCreation::builder()
            .name(table.to_string())
            .schema(iceberg_schema)
            .build();
        let created = self
            .catalog
            .create_table(&ns_ident, table_creation)
            .await
            .map_err(map_iceberg_error)?;
        info!(namespace = %namespace, table = %table, "Created table");
        table_info_from(namespace, table, &created)
    }

    async fn current_snapshot_id(&self, namespace: &str, table: &str) -> Result<Option<i64>> {
        let ident = table_ident(namespace, table)?;
        let loaded = self
            .catalog
            .load_table(&ident)
            .await
            .map_err(map_iceberg_error)?;
        Ok(loaded.metadata().current_snapshot_id())
    }

    async fn commit_snapshot(
        &self,
        namespace: &str,
        table: &str,
        commit: SnapshotCommit,
    ) -> Result<SnapshotCommitResult> {
        if !commit.files_to_remove.is_empty() {
            return Err(Error::Iceberg(IcebergError::SnapshotCommit(format!(
                "official REST catalog append commits do not support removing files; \
                 refusing to ignore {} requested removals",
                commit.files_to_remove.len()
            ))));
        }

        let files_added = commit.files_to_add.len();

        let result = commit_append_with_catalog_at_snapshot(
            &self.catalog,
            &self.warehouse_path,
            namespace,
            table,
            commit.expected_snapshot_id,
            commit.files_to_add,
            commit.summary,
        )
        .await?;
        self.remember_manifest_list(&result);

        Ok(SnapshotCommitResult {
            snapshot_id: result.snapshot_id,
            committed_at: chrono::Utc::now(),
            files_added,
            files_removed: 0,
        })
    }

    fn manifest_list_path_for_snapshot(&self, snapshot_id: i64) -> Option<String> {
        self.recent_manifest_lists
            .lock()
            .iter()
            .find(|(candidate, _)| *candidate == snapshot_id)
            .map(|(_, path)| path.clone())
    }

    async fn update_schema(
        &self,
        namespace: &str,
        table: &str,
        schema: &TableSchema,
        expected_schema_id: Option<i32>,
    ) -> Result<TableInfo> {
        self.update_schema_standalone(namespace, table, schema, expected_schema_id)
            .await
    }

    fn catalog_type(&self) -> CatalogType {
        CatalogType::Rest
    }

    fn warehouse_path(&self) -> &str {
        &self.warehouse_path
    }

    async fn close(&self) -> Result<()> {
        *self.oauth2_token.write() = None;
        info!("Official REST committer closed");
        Ok(())
    }
}

/// Extract the warehouse prefix from a catalog `/v1/config` response.
///
/// Per the Iceberg REST spec, `overrides` take precedence over `defaults`.
/// Returns `None` for single-warehouse catalogs that omit `prefix`.
fn extract_prefix(config: &rest_api::CatalogConfig) -> Option<String> {
    config
        .overrides
        .get("prefix")
        .or_else(|| config.defaults.get("prefix"))
        .cloned()
}

/// The official REST client treats only an `overrides.uri` value as a runtime
/// endpoint replacement; mirror that behavior in the temporary schema path.
fn extract_uri(config: &rest_api::CatalogConfig) -> Option<String> {
    config.overrides.get("uri").cloned()
}

/// Build the REST URL for a table commit (used by the standalone
/// `update_schema` path). When the catalog negotiated a warehouse `prefix`,
/// it is inserted between `/v1/` and `namespaces/` so multi-warehouse
/// catalogs (Lakekeeper, Polaris, Unity, Gravitino) address the right tenant.
fn build_update_schema_url(
    rest_uri: &str,
    prefix: Option<&str>,
    namespace: &str,
    table: &str,
) -> Result<String> {
    let namespace = namespace_ident(namespace)?;
    let namespace_path = namespace.to_url_string();
    let encoded_ns = urlencoding::encode(&namespace_path);
    let encoded_table = urlencoding::encode(table);
    let rest_uri = rest_uri.trim_end_matches('/');
    Ok(match prefix {
        Some(p) => format!(
            "{}/v1/{}/namespaces/{}/tables/{}",
            rest_uri, p, encoded_ns, encoded_table
        ),
        None => format!(
            "{}/v1/namespaces/{}/tables/{}",
            rest_uri, encoded_ns, encoded_table
        ),
    })
}

/// Resolve the OAuth2 token endpoint exactly as the official REST client does:
/// an explicit endpoint is already complete, while the catalog URI uses the
/// Iceberg REST default path.
fn oauth2_token_url(rest_uri: &str, explicit_endpoint: Option<&str>) -> String {
    explicit_endpoint
        .map(str::to_string)
        .unwrap_or_else(|| format!("{}/v1/oauth/tokens", rest_uri.trim_end_matches('/')))
}

fn custom_header_map(headers: &HashMap<String, String>) -> Result<HeaderMap> {
    let mut parsed = HeaderMap::new();
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| Error::Config(format!("Invalid REST custom header name {name:?}: {e}")))?;
        let value = HeaderValue::from_bytes(value.as_bytes()).map_err(|e| {
            Error::Config(format!("Invalid value for REST custom header {name}: {e}"))
        })?;
        parsed.insert(name, value);
    }
    Ok(parsed)
}

/// Convert a K2I `field_type` string to a `serde_json::Value` suitable for
/// the REST API schema representation.
fn schema_type_to_api_value(field_type: &str) -> serde_json::Value {
    let trimmed = field_type.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        serde_json::from_str(trimmed)
            .unwrap_or_else(|_| serde_json::Value::String(field_type.to_string()))
    } else {
        serde_json::Value::String(field_type.to_string())
    }
}

/// Convert a dot-separated namespace string to an Iceberg `NamespaceIdent`.
fn namespace_ident(namespace: &str) -> Result<::iceberg::NamespaceIdent> {
    let parts: Vec<&str> = namespace.split('.').filter(|p| !p.is_empty()).collect();
    ::iceberg::NamespaceIdent::from_strs(parts).map_err(map_iceberg_error)
}

/// Convert an official Iceberg `Schema` back into K2I's `TableSchema`.
fn iceberg_schema_to_table_schema(schema: &::iceberg::spec::Schema) -> Result<TableSchema> {
    Ok(TableSchema {
        schema_id: schema.schema_id(),
        fields: schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| {
                let value = serde_json::to_value(&field.field_type).map_err(|e| {
                    Error::Iceberg(IcebergError::SchemaEvolution(format!(
                        "failed to serialize Iceberg type for field {}: {}",
                        field.name, e
                    )))
                })?;
                let field_type = match value {
                    serde_json::Value::String(value) => value,
                    value => value.to_string(),
                };
                Ok(SchemaFieldInfo {
                    id: field.id,
                    name: field.name.clone(),
                    field_type,
                    required: field.required,
                    doc: field.doc.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?,
    })
}

/// Build a `TableInfo` from an official Iceberg `Table`.
fn table_info_from(
    namespace: &str,
    table: &str,
    table_ref: &::iceberg::table::Table,
) -> Result<TableInfo> {
    let metadata = table_ref.metadata();
    let schema = iceberg_schema_to_table_schema(metadata.current_schema())?;
    Ok(TableInfo {
        namespace: namespace.to_string(),
        name: table.to_string(),
        location: metadata.location().to_string(),
        current_snapshot_id: metadata.current_snapshot_id(),
        schema,
        properties: metadata.properties().clone(),
    })
}

/// Commit existing Parquet files with any official Iceberg catalog.
pub async fn commit_append_with_catalog(
    catalog: &dyn ::iceberg::Catalog,
    warehouse_path: &str,
    namespace: &str,
    table: &str,
    files: Vec<DataFileInfo>,
    summary: HashMap<String, String>,
) -> Result<OfficialCommitResult> {
    commit_append_with_catalog_at_snapshot(
        catalog,
        warehouse_path,
        namespace,
        table,
        None,
        files,
        summary,
    )
    .await
}

/// Commit an append while enforcing the caller's expected snapshot before
/// constructing the official Iceberg transaction. The transaction adds a
/// second CAS boundary between the loaded metadata and the REST commit.
async fn commit_append_with_catalog_at_snapshot(
    catalog: &dyn ::iceberg::Catalog,
    warehouse_path: &str,
    namespace: &str,
    table: &str,
    expected_snapshot_id: Option<i64>,
    files: Vec<DataFileInfo>,
    mut summary: HashMap<String, String>,
) -> Result<OfficialCommitResult> {
    use ::iceberg::transaction::{ApplyTransactionAction, Transaction};
    if files.is_empty() {
        return Err(Error::Iceberg(IcebergError::SnapshotCommit(
            "cannot append an empty file set".into(),
        )));
    }

    // Remove "operation" from the summary HashMap before passing to the
    // official iceberg-rust crate.  The crate's Summary struct has `operation`
    // as a named field plus `#[serde(flatten)] additional_properties`.  If
    // "operation" is left in the HashMap, it gets serialized twice (once from
    // the struct field, once from the flattened map), causing "duplicate field
    // `operation`" errors in catalogs like Lakekeeper.
    //
    // See: https://github.com/apache/iceberg/issues/9837
    summary.remove("operation");

    let ident = table_ident(namespace, table)?;
    let loaded = catalog
        .load_table(&ident)
        .await
        .map_err(map_iceberg_error)?;
    if let Some(expected) = expected_snapshot_id {
        let actual = loaded.metadata().current_snapshot_id();
        if actual != Some(expected) {
            return Err(Error::Iceberg(IcebergError::CasConflict {
                expected,
                actual: actual.unwrap_or(-1),
            }));
        }
    }
    let spec_id = loaded.metadata().default_partition_spec_id();
    let data_files = files
        .iter()
        .map(|file| data_file_from_info(file, warehouse_path, spec_id))
        .collect::<Result<Vec<_>>>()?;

    let tx = Transaction::new(&loaded);
    let action = tx
        .fast_append()
        .set_snapshot_properties(summary)
        .add_data_files(data_files);
    let tx = action.apply(tx).map_err(map_iceberg_error)?;
    let committed = tx.commit(catalog).await.map_err(map_iceberg_error)?;
    let snapshot = committed.metadata().current_snapshot().ok_or_else(|| {
        Error::Iceberg(IcebergError::SnapshotCommit(
            "append commit completed without a current snapshot".into(),
        ))
    })?;

    Ok(OfficialCommitResult {
        snapshot_id: snapshot.snapshot_id(),
        manifest_list_path: snapshot.manifest_list().to_string(),
    })
}

/// Convert a K2I table schema to an official Iceberg schema.
pub fn table_schema_to_iceberg_schema(schema: &TableSchema) -> Result<::iceberg::spec::Schema> {
    let fields = schema
        .fields
        .iter()
        .map(nested_field_from_info)
        .collect::<Result<Vec<_>>>()?;

    ::iceberg::spec::Schema::builder()
        .with_schema_id(schema.schema_id)
        .with_fields(fields)
        .build()
        .map_err(map_iceberg_error)
}

fn nested_field_from_info(field: &SchemaFieldInfo) -> Result<::iceberg::spec::NestedFieldRef> {
    let field_type = parse_iceberg_type(&field.field_type)?;
    let mut nested = if field.required {
        ::iceberg::spec::NestedField::required(field.id, &field.name, field_type)
    } else {
        ::iceberg::spec::NestedField::optional(field.id, &field.name, field_type)
    };
    nested.doc = field.doc.clone();
    Ok(Arc::new(nested))
}

fn parse_iceberg_type(field_type: &str) -> Result<::iceberg::spec::Type> {
    use ::iceberg::spec::{PrimitiveType, Type};

    let trimmed = field_type.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return serde_json::from_str::<Type>(trimmed)
            .map_err(|e| Error::Iceberg(IcebergError::SchemaEvolution(e.to_string())));
    }

    let lower = trimmed.to_ascii_lowercase();
    let primitive = match lower.as_str() {
        "boolean" | "bool" => PrimitiveType::Boolean,
        "int" | "integer" => PrimitiveType::Int,
        "long" | "bigint" => PrimitiveType::Long,
        "float" => PrimitiveType::Float,
        "double" => PrimitiveType::Double,
        "date" => PrimitiveType::Date,
        "time" => PrimitiveType::Time,
        "timestamp" => PrimitiveType::Timestamp,
        "timestamptz" => PrimitiveType::Timestamptz,
        "timestamp_ns" => PrimitiveType::TimestampNs,
        "timestamptz_ns" => PrimitiveType::TimestamptzNs,
        "string" | "utf8" => PrimitiveType::String,
        "uuid" => PrimitiveType::Uuid,
        "binary" => PrimitiveType::Binary,
        _ if lower.starts_with("decimal(") && lower.ends_with(')') => {
            let args = &lower["decimal(".len()..lower.len() - 1];
            let (precision, scale) = args.split_once(',').ok_or_else(|| {
                Error::Iceberg(IcebergError::SchemaEvolution(format!(
                    "invalid decimal type: {}",
                    field_type
                )))
            })?;
            PrimitiveType::Decimal {
                precision: precision.trim().parse().map_err(|e| {
                    Error::Iceberg(IcebergError::SchemaEvolution(format!(
                        "invalid decimal precision {}: {}",
                        precision, e
                    )))
                })?,
                scale: scale.trim().parse().map_err(|e| {
                    Error::Iceberg(IcebergError::SchemaEvolution(format!(
                        "invalid decimal scale {}: {}",
                        scale, e
                    )))
                })?,
            }
        }
        _ if lower.starts_with("fixed[") && lower.ends_with(']') => {
            let len = &lower["fixed[".len()..lower.len() - 1];
            PrimitiveType::Fixed(len.parse().map_err(|e| {
                Error::Iceberg(IcebergError::SchemaEvolution(format!(
                    "invalid fixed length {}: {}",
                    len, e
                )))
            })?)
        }
        _ => {
            return Err(Error::Iceberg(IcebergError::SchemaEvolution(format!(
                "unsupported Iceberg type: {}",
                field_type
            ))));
        }
    };

    Ok(Type::Primitive(primitive))
}

fn data_file_from_info(
    file: &DataFileInfo,
    warehouse_path: &str,
    partition_spec_id: i32,
) -> Result<::iceberg::spec::DataFile> {
    use ::iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};

    let format = match file.file_format.to_ascii_lowercase().as_str() {
        "parquet" => DataFileFormat::Parquet,
        "avro" => DataFileFormat::Avro,
        "orc" => DataFileFormat::Orc,
        other => {
            return Err(Error::Iceberg(IcebergError::SnapshotCommit(format!(
                "unsupported data file format: {}",
                other
            ))));
        }
    };

    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(data_file_uri(&file.file_path, warehouse_path)?)
        .file_format(format)
        .file_size_in_bytes(file.file_size_bytes)
        .record_count(file.record_count)
        .partition_spec_id(partition_spec_id)
        .partition(Struct::empty())
        .build()
        .map_err(|e| Error::Iceberg(IcebergError::SnapshotCommit(e.to_string())))
}

fn table_ident(namespace: &str, table: &str) -> Result<::iceberg::TableIdent> {
    let parts = namespace
        .split('.')
        .filter(|part| !part.is_empty())
        .chain(std::iter::once(table))
        .collect::<Vec<_>>();
    ::iceberg::TableIdent::from_strs(parts).map_err(map_iceberg_error)
}

fn warehouse_uri(warehouse_path: &str) -> Result<String> {
    if warehouse_path.contains("://") {
        return Ok(warehouse_path.trim_end_matches('/').to_string());
    }

    let path = Path::new(warehouse_path);
    let absolute = if path.is_absolute() {
        PathBuf::from(path)
    } else {
        std::env::current_dir()?.join(path)
    };

    Ok(format!("file://{}", absolute.to_string_lossy())
        .trim_end_matches('/')
        .to_string())
}

fn data_file_uri(file_path: &str, warehouse_path: &str) -> Result<String> {
    if file_path.contains("://") {
        return Ok(file_path.to_string());
    }

    let path = Path::new(file_path);
    if path.is_absolute() {
        return Ok(format!("file://{}", path.to_string_lossy()));
    }

    let warehouse = warehouse_uri(warehouse_path)?;
    Ok(format!(
        "{}/{}",
        warehouse.trim_end_matches('/'),
        file_path.trim_start_matches('/')
    ))
}

fn apply_rest_auth_props(
    config: &IcebergConfig,
    props: &mut HashMap<String, String>,
) -> Result<()> {
    match config.rest.credential_type {
        CredentialType::None => {}
        CredentialType::Bearer => {
            let token = config.rest.credential.as_ref().ok_or_else(|| {
                Error::Config("REST bearer auth requires iceberg.rest.credential".into())
            })?;
            props.insert("token".to_string(), token.clone());
        }
        CredentialType::OAuth2 => {
            let client_secret = config.rest.oauth2_client_secret.as_ref().ok_or_else(|| {
                Error::Config("REST OAuth2 requires iceberg.rest.oauth2_client_secret".into())
            })?;
            let credential = match &config.rest.oauth2_client_id {
                Some(client_id) => format!("{}:{}", client_id, client_secret),
                None => client_secret.clone(),
            };
            props.insert("credential".to_string(), credential);
            if let Some(endpoint) = &config.rest.oauth2_token_endpoint {
                props.insert("oauth2-server-uri".to_string(), endpoint.clone());
            }
            if let Some(scope) = &config.rest.oauth2_scope {
                props.insert("scope".to_string(), scope.clone());
            }
        }
    }

    Ok(())
}

fn apply_file_io_props(config: &IcebergConfig, props: &mut HashMap<String, String>) {
    if let Some(endpoint) = &config.s3_endpoint {
        props.insert("s3.endpoint".to_string(), endpoint.clone());
    }
    if let Some(access_key) = &config.aws_access_key_id {
        props.insert("s3.access-key-id".to_string(), access_key.clone());
    }
    if let Some(secret_key) = &config.aws_secret_access_key {
        props.insert("s3.secret-access-key".to_string(), secret_key.clone());
    }
    if let Some(region) = &config.aws_region {
        props.insert("s3.region".to_string(), region.clone());
    }
}

fn map_iceberg_error(err: ::iceberg::Error) -> Error {
    let message = err.to_string();
    match err.kind() {
        ::iceberg::ErrorKind::TableNotFound => Error::Iceberg(IcebergError::TableNotFound(message)),
        ::iceberg::ErrorKind::CatalogCommitConflicts => Error::Iceberg(IcebergError::CasConflict {
            expected: -1,
            actual: -1,
        }),
        _ => Error::Iceberg(IcebergError::Other(message)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    use ::iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
    use axum::http::HeaderMap as AxumHeaderMap;
    use axum::routing::get;
    use axum::{Json, Router};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn simple_schema() -> TableSchema {
        TableSchema {
            schema_id: 1,
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
                    name: "driver".into(),
                    field_type: "string".into(),
                    required: false,
                    doc: Some("driver name".into()),
                },
            ],
        }
    }

    fn rest_config(rest_uri: &str) -> IcebergConfig {
        toml::from_str(&format!(
            r#"
catalog_type = "rest"
warehouse_path = "/tmp/k2i-official-tests"
database_name = "analytics"
table_name = "events"
rest_uri = {rest_uri:?}
"#
        ))
        .unwrap()
    }

    #[test]
    fn warehouse_uri_converts_local_path() {
        assert_eq!(warehouse_uri("/tmp/k2i-wh").unwrap(), "file:///tmp/k2i-wh");
        assert_eq!(
            warehouse_uri("s3://warehouse/prefix/").unwrap(),
            "s3://warehouse/prefix"
        );
    }

    #[test]
    fn data_file_uri_prefixes_relative_local_path() {
        assert_eq!(
            data_file_uri("data/f1/part.parquet", "/tmp/k2i-wh").unwrap(),
            "file:///tmp/k2i-wh/data/f1/part.parquet"
        );
        assert_eq!(
            data_file_uri("/tmp/source.parquet", "/tmp/k2i-wh").unwrap(),
            "file:///tmp/source.parquet"
        );
    }

    #[test]
    fn converts_table_schema_to_official_schema() {
        let schema = table_schema_to_iceberg_schema(&simple_schema()).unwrap();
        assert_eq!(schema.schema_id(), 1);
        assert_eq!(schema.highest_field_id(), 2);
        assert_eq!(
            schema.field_by_name("driver").unwrap().doc.as_deref(),
            Some("driver name")
        );
    }

    #[tokio::test]
    async fn commits_append_with_memory_catalog() {
        let temp = tempfile::tempdir().unwrap();
        let warehouse = warehouse_uri(temp.path().to_str().unwrap()).unwrap();
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
            )
            .await
            .unwrap();
        let namespace = NamespaceIdent::from_strs(["f1"]).unwrap();
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = table_schema_to_iceberg_schema(&simple_schema()).unwrap();
        let table_creation = TableCreation::builder()
            .name("derived_state".to_string())
            .location(format!("{}/f1/derived_state", warehouse))
            .schema(schema)
            .build();
        catalog
            .create_table(&namespace, table_creation)
            .await
            .unwrap();

        let result = commit_append_with_catalog(
            &catalog,
            temp.path().to_str().unwrap(),
            "f1",
            "derived_state",
            vec![DataFileInfo {
                file_path: "data/f1/derived_state/part-1.parquet".into(),
                file_size_bytes: 128,
                record_count: 3,
                partition_values: HashMap::new(),
                file_format: "parquet".into(),
            }],
            HashMap::from([("operation".into(), "append".into())]),
        )
        .await
        .unwrap();

        assert!(result.snapshot_id != 0);
        assert!(result.manifest_list_path.contains("/metadata/snap-"));
        assert!(result.manifest_list_path.ends_with(".avro"));

        let table = catalog
            .load_table(&table_ident("f1", "derived_state").unwrap())
            .await
            .expect("committed table metadata should reload after snapshot serialization");
        let summary = table
            .metadata()
            .current_snapshot()
            .expect("committed table should have a current snapshot")
            .summary();
        assert!(
            !summary.additional_properties.contains_key("operation"),
            "`operation` must not be forwarded as a flattened snapshot property"
        );

        let json = serde_json::to_string(summary).unwrap();
        let round_tripped: ::iceberg::spec::Summary =
            serde_json::from_str(&json).expect("snapshot summary should round-trip as JSON");
        assert_eq!(round_tripped.operation, ::iceberg::spec::Operation::Append);

        let stale_snapshot_id = result.snapshot_id.saturating_add(1);
        let error = commit_append_with_catalog_at_snapshot(
            &catalog,
            temp.path().to_str().unwrap(),
            "f1",
            "derived_state",
            Some(stale_snapshot_id),
            vec![DataFileInfo {
                file_path: "data/f1/derived_state/part-2.parquet".into(),
                file_size_bytes: 64,
                record_count: 1,
                partition_values: HashMap::new(),
                file_format: "parquet".into(),
            }],
            HashMap::new(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            Error::Iceberg(IcebergError::CasConflict { expected, actual })
                if expected == stale_snapshot_id && actual == result.snapshot_id
        ));
    }

    #[test]
    fn test_namespace_ident() {
        let ident = namespace_ident("db.schema").unwrap();
        assert_eq!(ident.inner(), vec!["db", "schema"]);
    }

    #[test]
    fn test_namespace_ident_single() {
        let ident = namespace_ident("db").unwrap();
        assert_eq!(ident.inner(), vec!["db"]);
    }

    #[test]
    fn test_iceberg_schema_to_table_schema_roundtrip() {
        let original = simple_schema();
        let official = table_schema_to_iceberg_schema(&original).unwrap();
        let back = iceberg_schema_to_table_schema(&official).unwrap();
        assert_eq!(back.schema_id, original.schema_id);
        assert_eq!(back.fields.len(), original.fields.len());
        assert_eq!(back.fields[0].name, original.fields[0].name);
        assert_eq!(back.fields[0].field_type, original.fields[0].field_type);
    }

    #[test]
    fn test_complex_iceberg_types_roundtrip_as_canonical_json() {
        let original = TableSchema {
            schema_id: 1,
            fields: vec![
                SchemaFieldInfo {
                    id: 1,
                    name: "telemetry".into(),
                    field_type: serde_json::json!({
                        "type": "struct",
                        "fields": [
                            {
                                "id": 2,
                                "name": "speed",
                                "required": false,
                                "type": "double"
                            },
                            {
                                "id": 3,
                                "name": "gear",
                                "required": false,
                                "type": "int"
                            }
                        ]
                    })
                    .to_string(),
                    required: false,
                    doc: None,
                },
                SchemaFieldInfo {
                    id: 4,
                    name: "samples".into(),
                    field_type: serde_json::json!({
                        "type": "list",
                        "element-id": 5,
                        "element-required": false,
                        "element": "double"
                    })
                    .to_string(),
                    required: false,
                    doc: None,
                },
                SchemaFieldInfo {
                    id: 6,
                    name: "counters".into(),
                    field_type: serde_json::json!({
                        "type": "map",
                        "key-id": 7,
                        "key": "string",
                        "value-id": 8,
                        "value-required": false,
                        "value": "int"
                    })
                    .to_string(),
                    required: false,
                    doc: None,
                },
            ],
        };

        let official = table_schema_to_iceberg_schema(&original).unwrap();
        let round_tripped = iceberg_schema_to_table_schema(&official).unwrap();
        assert_eq!(round_tripped, original);
    }

    fn catalog_config(
        defaults: &[(&str, &str)],
        overrides: &[(&str, &str)],
    ) -> rest_api::CatalogConfig {
        rest_api::CatalogConfig {
            defaults: defaults
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            overrides: overrides
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn test_extract_prefix_from_overrides() {
        let config = catalog_config(&[], &[("prefix", "ws-a")]);
        assert_eq!(extract_prefix(&config), Some("ws-a".to_string()));
    }

    #[test]
    fn test_extract_prefix_from_defaults() {
        let config = catalog_config(&[("prefix", "ws-default")], &[]);
        assert_eq!(extract_prefix(&config), Some("ws-default".to_string()));
    }

    #[test]
    fn test_extract_prefix_overrides_win_over_defaults() {
        let config = catalog_config(&[("prefix", "ws-default")], &[("prefix", "ws-override")]);
        assert_eq!(extract_prefix(&config), Some("ws-override".to_string()));
    }

    #[test]
    fn test_extract_prefix_absent_is_none() {
        let config = catalog_config(&[], &[]);
        assert_eq!(extract_prefix(&config), None);
    }

    #[test]
    fn test_extract_uri_only_accepts_runtime_override() {
        let default_only = catalog_config(&[("uri", "http://default")], &[]);
        assert_eq!(extract_uri(&default_only), None);

        let overridden = catalog_config(&[], &[("uri", "http://override")]);
        assert_eq!(extract_uri(&overridden), Some("http://override".into()));
    }

    #[test]
    fn test_build_update_schema_url_with_prefix() {
        let url =
            build_update_schema_url("http://catalog:8181", Some("ws-a"), "analytics", "events")
                .unwrap();
        assert_eq!(
            url,
            "http://catalog:8181/v1/ws-a/namespaces/analytics/tables/events"
        );
    }

    #[test]
    fn test_build_update_schema_url_without_prefix() {
        let url =
            build_update_schema_url("http://catalog:8181", None, "analytics", "events").unwrap();
        assert_eq!(
            url,
            "http://catalog:8181/v1/namespaces/analytics/tables/events"
        );
    }

    #[test]
    fn test_build_update_schema_url_encodes_namespace_and_table() {
        // The prefix is inserted verbatim (already URL-safe as returned by the
        // catalog); only the namespace and table components are encoded.
        let url = build_update_schema_url("http://catalog:8181", Some("ws-a"), "my ns", "my table")
            .unwrap();
        assert_eq!(
            url,
            "http://catalog:8181/v1/ws-a/namespaces/my%20ns/tables/my%20table"
        );
    }

    #[test]
    fn test_build_update_schema_url_uses_rest_namespace_separator() {
        let url = build_update_schema_url(
            "http://catalog:8181/",
            Some("ws-a"),
            "org.analytics",
            "events",
        )
        .unwrap();
        assert_eq!(
            url,
            "http://catalog:8181/v1/ws-a/namespaces/org%1Fanalytics/tables/events"
        );
    }

    #[test]
    fn test_oauth2_token_url_respects_complete_override() {
        assert_eq!(
            oauth2_token_url("http://catalog:8181/", None),
            "http://catalog:8181/v1/oauth/tokens"
        );
        assert_eq!(
            oauth2_token_url(
                "http://catalog:8181",
                Some("https://identity.example.test/oauth/token")
            ),
            "https://identity.example.test/oauth/token"
        );
    }

    #[test]
    fn test_custom_header_map_rejects_invalid_names() {
        let error = custom_header_map(&HashMap::from([(
            "not a header".to_string(),
            "value".to_string(),
        )]))
        .unwrap_err();
        assert!(matches!(error, Error::Config(_)));
    }

    #[tokio::test]
    async fn resolves_runtime_route_once_for_concurrent_callers() {
        let calls = Arc::new(AtomicUsize::new(0));
        let saw_custom_header = Arc::new(AtomicBool::new(false));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let runtime_uri = format!("http://{address}/runtime");

        let app = Router::new().route(
            "/v1/config",
            get({
                let calls = calls.clone();
                let saw_custom_header = saw_custom_header.clone();
                let runtime_uri = runtime_uri.clone();
                move |headers: AxumHeaderMap| {
                    let calls = calls.clone();
                    let saw_custom_header = saw_custom_header.clone();
                    let runtime_uri = runtime_uri.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        if headers
                            .get("x-k2i-test")
                            .and_then(|value| value.to_str().ok())
                            == Some("route")
                        {
                            saw_custom_header.store(true, Ordering::SeqCst);
                        }
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        Json(serde_json::json!({
                            "defaults": {},
                            "overrides": {
                                "prefix": "warehouse-a",
                                "uri": runtime_uri,
                            }
                        }))
                    }
                }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut config = rest_config(&format!("http://{address}"));
        config
            .rest
            .custom_headers
            .insert("x-k2i-test".into(), "route".into());
        let committer = OfficialRestCommitter::new(&config).await.unwrap();

        let (first, second, third) = tokio::join!(
            committer.do_resolve_route(),
            committer.do_resolve_route(),
            committer.do_resolve_route()
        );
        let expected = ResolvedRestRoute {
            base_uri: runtime_uri,
            prefix: Some("warehouse-a".into()),
        };
        assert_eq!(first.unwrap(), expected);
        assert_eq!(second.unwrap(), expected);
        assert_eq!(third.unwrap(), expected);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(saw_custom_header.load(Ordering::SeqCst));

        server.abort();
    }

    #[tokio::test]
    async fn rejects_removals_and_keeps_manifest_cache_bounded() {
        let committer = OfficialRestCommitter::new(&rest_config("http://127.0.0.1:1"))
            .await
            .unwrap();

        for snapshot_id in 0..=RECENT_MANIFEST_LIST_LIMIT as i64 {
            committer.remember_manifest_list(&OfficialCommitResult {
                snapshot_id,
                manifest_list_path: format!("metadata/snap-{snapshot_id}.avro"),
            });
        }
        assert_eq!(committer.manifest_list_path_for_snapshot(0), None);
        assert_eq!(
            committer.manifest_list_path_for_snapshot(RECENT_MANIFEST_LIST_LIMIT as i64),
            Some(format!("metadata/snap-{}.avro", RECENT_MANIFEST_LIST_LIMIT))
        );

        let error = committer
            .commit_snapshot(
                "analytics",
                "events",
                SnapshotCommit {
                    expected_snapshot_id: None,
                    files_to_add: vec![],
                    files_to_remove: vec!["data/old.parquet".into()],
                    summary: HashMap::new(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::Iceberg(IcebergError::SnapshotCommit(message))
                if message.contains("refusing to ignore 1 requested removals")
        ));
    }
}
