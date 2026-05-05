//! Schema Registry abstraction for Protobuf decoding.

use crate::{Error, Result};
use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::warn;

/// Schema reference returned by Confluent Schema Registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaReference {
    /// Import name in the parent `.proto`.
    pub name: String,
    /// Referenced subject.
    pub subject: String,
    /// Referenced version.
    pub version: i32,
}

/// Registered Protobuf schema plus optional descriptor-set bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredSchema {
    /// Schema Registry ID.
    pub id: i32,
    /// Subject, when fetched by subject/version.
    pub subject: Option<String>,
    /// Version, when fetched by subject/version.
    pub version: Option<i32>,
    /// Schema type, expected to be `PROTOBUF`.
    pub schema_type: String,
    /// Raw `.proto` source text from Schema Registry.
    pub schema: String,
    /// Import/reference metadata.
    pub references: Vec<SchemaReference>,
    /// Optional encoded `prost_types::FileDescriptorSet`.
    pub descriptor_set_bytes: Option<Vec<u8>>,
    /// Optional configured/resolved fully-qualified message type.
    pub message_type: Option<String>,
}

/// Async registry client boundary.
#[async_trait]
pub trait SchemaRegistryClient: Send + Sync {
    /// Fetch schema by global registry ID.
    async fn get_by_id(&self, id: i32) -> Result<RegisteredSchema>;

    /// Fetch latest schema by subject.
    async fn get_latest(&self, subject: &str) -> Result<RegisteredSchema>;

    /// Fetch a specific subject version.
    async fn get_version(&self, subject: &str, version: i32) -> Result<RegisteredSchema>;
}

/// Minimal Confluent Schema Registry HTTP client with in-memory TTL cache.
#[derive(Debug)]
pub struct HttpSchemaRegistryClient {
    base_url: String,
    client: reqwest::Client,
    ttl: Duration,
    disk_cache_dir: Option<PathBuf>,
    by_id: RwLock<HashMap<i32, CacheEntry>>,
    by_subject: RwLock<HashMap<String, CacheEntry>>,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    schema: RegisteredSchema,
    fetched_at: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiskCacheEntry {
    schema: RegisteredSchema,
    cached_at_ms: u128,
}

impl CacheEntry {
    fn is_valid(&self, ttl: Duration) -> bool {
        self.fetched_at.elapsed() <= ttl
    }
}

impl HttpSchemaRegistryClient {
    /// Create a Schema Registry client.
    pub fn new(base_url: impl Into<String>, ttl: Duration) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
            ttl,
            disk_cache_dir: None,
            by_id: RwLock::new(HashMap::new()),
            by_subject: RwLock::new(HashMap::new()),
        }
    }

    /// Create a Schema Registry client that keeps stale schema copies on disk.
    ///
    /// The HTTP path is still preferred whenever available. The disk cache is a
    /// last-good fallback for startup or transient registry outages.
    pub fn with_disk_cache(
        base_url: impl Into<String>,
        ttl: Duration,
        disk_cache_dir: impl Into<PathBuf>,
    ) -> Self {
        let mut client = Self::new(base_url, ttl);
        client.disk_cache_dir = Some(disk_cache_dir.into());
        client
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T> {
        let url = format!("{}/{}", self.base_url, path.trim_start_matches('/'));
        let response =
            self.client.get(&url).send().await.map_err(|e| {
                Error::Serialization(format!("schema registry request failed: {}", e))
            })?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Serialization(format!(
                "schema registry request {} failed with {}: {}",
                url, status, body
            )));
        }

        response.json::<T>().await.map_err(|e| {
            Error::Serialization(format!("schema registry response decode failed: {}", e))
        })
    }

    fn id_cache_name(id: i32) -> String {
        format!("id-{}.json", id)
    }

    fn latest_subject_cache_name(subject: &str) -> String {
        format!("subject-{}-latest.json", urlencoding::encode(subject))
    }

    fn version_subject_cache_name(subject: &str, version: i32) -> String {
        format!(
            "subject-{}-version-{}.json",
            urlencoding::encode(subject),
            version
        )
    }

    fn cache_path(&self, name: &str) -> Option<PathBuf> {
        self.disk_cache_dir.as_ref().map(|dir| dir.join(name))
    }

    fn cache_entry(schema: RegisteredSchema) -> CacheEntry {
        CacheEntry {
            schema,
            fetched_at: Instant::now(),
        }
    }

    fn remember_id(&self, schema: &RegisteredSchema) {
        self.by_id
            .write()
            .insert(schema.id, Self::cache_entry(schema.clone()));
        self.write_disk_cache(&Self::id_cache_name(schema.id), schema);
    }

    fn remember_subject(&self, key: impl Into<String>, name: String, schema: &RegisteredSchema) {
        let entry = Self::cache_entry(schema.clone());
        self.by_id.write().insert(schema.id, entry.clone());
        self.by_subject.write().insert(key.into(), entry);
        self.write_disk_cache(&Self::id_cache_name(schema.id), schema);
        self.write_disk_cache(&name, schema);
    }

    fn write_disk_cache(&self, name: &str, schema: &RegisteredSchema) {
        let Some(path) = self.cache_path(name) else {
            return;
        };
        if let Err(error) = self.try_write_disk_cache(&path, schema) {
            warn!(
                path = %path.display(),
                error = %error,
                "Failed to write schema registry disk cache"
            );
        }
    }

    fn try_write_disk_cache(&self, path: &Path, schema: &RegisteredSchema) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                Error::Serialization(format!("failed to create schema cache directory: {}", e))
            })?;
        }

        let cached_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let entry = DiskCacheEntry {
            schema: schema.clone(),
            cached_at_ms,
        };
        let bytes = serde_json::to_vec_pretty(&entry).map_err(|e| {
            Error::Serialization(format!("failed to encode schema cache entry: {}", e))
        })?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, bytes).map_err(|e| {
            Error::Serialization(format!("failed to write schema cache entry: {}", e))
        })?;
        std::fs::rename(&tmp, path).map_err(|e| {
            Error::Serialization(format!("failed to publish schema cache entry: {}", e))
        })
    }

    fn read_disk_cache(&self, name: &str) -> Option<RegisteredSchema> {
        let path = self.cache_path(name)?;
        let bytes = std::fs::read(&path).ok()?;
        match serde_json::from_slice::<DiskCacheEntry>(&bytes) {
            Ok(entry) => Some(entry.schema),
            Err(error) => {
                warn!(
                    path = %path.display(),
                    error = %error,
                    "Ignoring invalid schema registry disk cache entry"
                );
                None
            }
        }
    }
}

#[async_trait]
impl SchemaRegistryClient for HttpSchemaRegistryClient {
    async fn get_by_id(&self, id: i32) -> Result<RegisteredSchema> {
        if let Some(entry) = self.by_id.read().get(&id) {
            if entry.is_valid(self.ttl) {
                return Ok(entry.schema.clone());
            }
        }

        let response: SchemaByIdResponse =
            match self.get_json(&format!("/schemas/ids/{}", id)).await {
                Ok(response) => response,
                Err(error) => {
                    if let Some(schema) = self.read_disk_cache(&Self::id_cache_name(id)) {
                        self.by_id
                            .write()
                            .insert(id, Self::cache_entry(schema.clone()));
                        return Ok(schema);
                    }
                    return Err(error);
                }
            };
        let schema = RegisteredSchema {
            id,
            subject: None,
            version: None,
            schema_type: response
                .schema_type
                .unwrap_or_else(|| "PROTOBUF".to_string()),
            schema: response.schema,
            references: response.references.unwrap_or_default(),
            descriptor_set_bytes: None,
            message_type: None,
        };
        self.remember_id(&schema);
        Ok(schema)
    }

    async fn get_latest(&self, subject: &str) -> Result<RegisteredSchema> {
        if let Some(entry) = self.by_subject.read().get(subject) {
            if entry.is_valid(self.ttl) {
                return Ok(entry.schema.clone());
            }
        }

        let encoded_subject = urlencoding::encode(subject);
        let response: SubjectVersionResponse = match self
            .get_json(&format!("/subjects/{}/versions/latest", encoded_subject))
            .await
        {
            Ok(response) => response,
            Err(error) => {
                if let Some(schema) =
                    self.read_disk_cache(&Self::latest_subject_cache_name(subject))
                {
                    self.remember_subject(
                        subject.to_string(),
                        Self::latest_subject_cache_name(subject),
                        &schema,
                    );
                    return Ok(schema);
                }
                return Err(error);
            }
        };
        let schema = RegisteredSchema {
            id: response.id,
            subject: Some(response.subject.clone()),
            version: Some(response.version),
            schema_type: response
                .schema_type
                .unwrap_or_else(|| "PROTOBUF".to_string()),
            schema: response.schema,
            references: response.references.unwrap_or_default(),
            descriptor_set_bytes: None,
            message_type: None,
        };

        self.remember_subject(
            response.subject.clone(),
            Self::latest_subject_cache_name(&response.subject),
            &schema,
        );
        Ok(schema)
    }

    async fn get_version(&self, subject: &str, version: i32) -> Result<RegisteredSchema> {
        let encoded_subject = urlencoding::encode(subject);
        let response: SubjectVersionResponse = match self
            .get_json(&format!(
                "/subjects/{}/versions/{}",
                encoded_subject, version
            ))
            .await
        {
            Ok(response) => response,
            Err(error) => {
                if let Some(schema) =
                    self.read_disk_cache(&Self::version_subject_cache_name(subject, version))
                {
                    self.remember_subject(
                        format!("{}:{}", subject, version),
                        Self::version_subject_cache_name(subject, version),
                        &schema,
                    );
                    return Ok(schema);
                }
                return Err(error);
            }
        };
        let schema = RegisteredSchema {
            id: response.id,
            subject: Some(response.subject.clone()),
            version: Some(response.version),
            schema_type: response
                .schema_type
                .unwrap_or_else(|| "PROTOBUF".to_string()),
            schema: response.schema,
            references: response.references.unwrap_or_default(),
            descriptor_set_bytes: None,
            message_type: None,
        };

        self.remember_subject(
            format!("{}:{}", response.subject, response.version),
            Self::version_subject_cache_name(&response.subject, response.version),
            &schema,
        );
        Ok(schema)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SchemaByIdResponse {
    schema: String,
    #[serde(default, rename = "schemaType")]
    schema_type: Option<String>,
    #[serde(default)]
    references: Option<Vec<SchemaReference>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubjectVersionResponse {
    subject: String,
    id: i32,
    version: i32,
    schema: String,
    #[serde(default, rename = "schemaType")]
    schema_type: Option<String>,
    #[serde(default)]
    references: Option<Vec<SchemaReference>>,
}

/// In-memory registry for unit and integration tests.
#[derive(Debug, Default)]
pub struct InMemorySchemaRegistryClient {
    by_id: RwLock<HashMap<i32, RegisteredSchema>>,
    latest_by_subject: RwLock<HashMap<String, i32>>,
    by_subject_version: RwLock<HashMap<(String, i32), i32>>,
}

impl InMemorySchemaRegistryClient {
    /// Insert a registered schema.
    pub fn insert(&self, schema: RegisteredSchema) {
        if let Some(subject) = &schema.subject {
            self.latest_by_subject
                .write()
                .insert(subject.clone(), schema.id);
            if let Some(version) = schema.version {
                self.by_subject_version
                    .write()
                    .insert((subject.clone(), version), schema.id);
            }
        }
        self.by_id.write().insert(schema.id, schema);
    }
}

#[async_trait]
impl SchemaRegistryClient for InMemorySchemaRegistryClient {
    async fn get_by_id(&self, id: i32) -> Result<RegisteredSchema> {
        self.by_id
            .read()
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::Serialization(format!("schema ID {} not found", id)))
    }

    async fn get_latest(&self, subject: &str) -> Result<RegisteredSchema> {
        let id = self
            .latest_by_subject
            .read()
            .get(subject)
            .copied()
            .ok_or_else(|| Error::Serialization(format!("subject '{}' not found", subject)))?;
        self.get_by_id(id).await
    }

    async fn get_version(&self, subject: &str, version: i32) -> Result<RegisteredSchema> {
        let id = self
            .by_subject_version
            .read()
            .get(&(subject.to_string(), version))
            .copied()
            .ok_or_else(|| {
                Error::Serialization(format!(
                    "subject '{}' version {} not found",
                    subject, version
                ))
            })?;
        self.get_by_id(id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_registry_fetches_by_id_and_subject() {
        let registry = InMemorySchemaRegistryClient::default();
        registry.insert(RegisteredSchema {
            id: 11,
            subject: Some("events-value".to_string()),
            version: Some(3),
            schema_type: "PROTOBUF".to_string(),
            schema: "syntax = \"proto3\"; message Event {}".to_string(),
            references: vec![],
            descriptor_set_bytes: Some(vec![1, 2, 3]),
            message_type: Some("test.Event".to_string()),
        });

        assert_eq!(registry.get_by_id(11).await.unwrap().id, 11);
        assert_eq!(
            registry.get_latest("events-value").await.unwrap().version,
            Some(3)
        );
    }

    #[tokio::test]
    async fn http_registry_falls_back_to_disk_cache_by_id() {
        let temp = tempfile::tempdir().unwrap();
        let registry = HttpSchemaRegistryClient::with_disk_cache(
            "http://127.0.0.1:9",
            Duration::ZERO,
            temp.path(),
        );
        let schema = RegisteredSchema {
            id: 42,
            subject: None,
            version: None,
            schema_type: "PROTOBUF".to_string(),
            schema: "syntax = \"proto3\"; message Event {}".to_string(),
            references: vec![],
            descriptor_set_bytes: None,
            message_type: Some("Event".to_string()),
        };

        registry.write_disk_cache(&HttpSchemaRegistryClient::id_cache_name(42), &schema);

        let fetched = registry.get_by_id(42).await.unwrap();
        assert_eq!(fetched, schema);
    }

    #[tokio::test]
    async fn http_registry_falls_back_to_disk_cache_for_subjects() {
        let temp = tempfile::tempdir().unwrap();
        let registry = HttpSchemaRegistryClient::with_disk_cache(
            "http://127.0.0.1:9",
            Duration::ZERO,
            temp.path(),
        );
        let schema = RegisteredSchema {
            id: 43,
            subject: Some("events-value".to_string()),
            version: Some(7),
            schema_type: "PROTOBUF".to_string(),
            schema: "syntax = \"proto3\"; message Event {}".to_string(),
            references: vec![],
            descriptor_set_bytes: None,
            message_type: Some("Event".to_string()),
        };

        registry.write_disk_cache(
            &HttpSchemaRegistryClient::latest_subject_cache_name("events-value"),
            &schema,
        );
        registry.write_disk_cache(
            &HttpSchemaRegistryClient::version_subject_cache_name("events-value", 7),
            &schema,
        );

        assert_eq!(registry.get_latest("events-value").await.unwrap(), schema);
        assert_eq!(
            registry.get_version("events-value", 7).await.unwrap(),
            schema
        );
    }
}
