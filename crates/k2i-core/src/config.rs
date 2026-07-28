//! Configuration structures for k2i.
//!
//! Configuration is loaded from TOML files and can be overridden via CLI flags.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// A configuration secret that redacts its value in `Debug` output.
///
/// In TOML it accepts either a plain string or a `{ file = "path" }` table,
/// which supports Kubernetes projected-volume secrets:
///
/// ```toml
/// sasl_password = "hunter2"
/// # or
/// sasl_password = { file = "/etc/secrets/k2i/kafka-password" }
/// ```
///
/// File contents are trimmed. Read the value explicitly with
/// [`Secret::expose`] or via `Deref` (`&*secret`).
///
/// # Output is redacted
///
/// Both `Debug` and [`Serialize`] redact: they emit [`Secret::REDACTED`], never
/// the value. `Config` derives `Serialize`, so any future code that dumps or
/// echoes the configuration — a `config dump` subcommand, an RPC response, a
/// diagnostic bundle — cannot leak a credential by accident. This mirrors the
/// `secrecy` crate, which deliberately does not implement `Serialize` for
/// secret-wrapped strings so that emitting one has to be a conscious act.
///
/// The trade-off is that a serialized `Config` does not round-trip: reading one
/// back yields the literal `REDACTED` marker rather than the original
/// credential. That is intentional — a visibly broken credential is a far better
/// failure than a silently leaked one. Code that genuinely needs the plaintext
/// must call [`Secret::expose`] at the point of use.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// The placeholder emitted by `Debug` and `Serialize` in place of the value.
    pub const REDACTED: &'static str = "REDACTED";

    /// Wrap a plaintext value as a secret.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Expose the secret value.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Secret({})", Self::REDACTED)
    }
}

impl Serialize for Secret {
    /// Emits [`Secret::REDACTED`] rather than the value. See the type docs for
    /// why this deliberately breaks round-tripping.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(Self::REDACTED)
    }
}

impl std::ops::Deref for Secret {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Plain(String),
            FileRef { file: PathBuf },
        }

        match Repr::deserialize(deserializer)? {
            Repr::Plain(value) => Ok(Secret(value)),
            Repr::FileRef { file } => {
                let contents = std::fs::read_to_string(&file).map_err(|e| {
                    serde::de::Error::custom(format!(
                        "failed to read secret file '{}': {}",
                        file.display(),
                        e
                    ))
                })?;
                Ok(Secret(contents.trim().to_string()))
            }
        }
    }
}

/// Main configuration structure.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// Kafka configuration
    pub kafka: KafkaConfig,

    /// Runtime schema evolution behavior
    #[serde(default)]
    pub schema_evolution: SchemaEvolutionRuntimeConfig,

    /// Iceberg configuration
    pub iceberg: IcebergConfig,

    /// Hot buffer configuration
    #[serde(default)]
    pub buffer: BufferConfig,

    /// Transaction log configuration
    #[serde(default)]
    pub transaction_log: TransactionLogConfig,

    /// Maintenance configuration
    #[serde(default)]
    pub maintenance: MaintenanceConfig,

    /// Monitoring configuration
    #[serde(default)]
    pub monitoring: MonitoringConfig,

    /// Real-time read RPC configuration
    #[serde(default)]
    pub rpc: RpcConfig,
}

/// Kafka consumer configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KafkaConfig {
    /// Kafka bootstrap servers
    pub bootstrap_servers: Vec<String>,

    /// Topic to consume from
    pub topic: String,

    /// Consumer group ID
    pub consumer_group: String,

    /// Batch size (number of messages per batch)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Batch timeout in milliseconds
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,

    /// Session timeout in milliseconds
    #[serde(default = "default_session_timeout_ms")]
    pub session_timeout_ms: u32,

    /// Heartbeat interval in milliseconds
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u32,

    /// Max poll interval in milliseconds (CRITICAL: must exceed longest flush time)
    #[serde(default = "default_max_poll_interval_ms")]
    pub max_poll_interval_ms: u32,

    /// Auto offset reset strategy
    #[serde(default)]
    pub auto_offset_reset: OffsetReset,

    /// Security configuration
    #[serde(default)]
    pub security: KafkaSecurityConfig,

    /// Kafka value format.
    #[serde(default)]
    pub format: KafkaFormatConfig,
}

/// Kafka value format configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum KafkaFormatConfig {
    /// Preserve raw Kafka key/value bytes.
    #[default]
    Raw,
    /// JSON value payloads.
    Json,
    /// Confluent Schema Registry Protobuf value payloads.
    Protobuf(ProtobufFormatConfig),
}

/// Protobuf format configuration.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ProtobufFormatConfig {
    /// Confluent Schema Registry URL.
    pub schema_registry_url: String,

    /// Optional subject strategy. Defaults to topic_name.
    #[serde(default)]
    pub subject_strategy: ProtobufSubjectStrategy,

    /// Fully-qualified Protobuf message type.
    #[serde(default)]
    pub message_type: Option<String>,

    /// Registry cache TTL in seconds.
    #[serde(default = "default_schema_registry_cache_ttl_seconds")]
    pub cache_ttl_seconds: u64,

    /// Fetch latest subject schema on startup.
    #[serde(default = "default_true")]
    pub latest_on_startup: bool,
}

/// Protobuf subject strategy.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ProtobufSubjectStrategy {
    /// `<topic>-value`.
    #[default]
    TopicName,
    /// Fully-qualified record name.
    RecordName,
    /// `<topic>-<record>`.
    TopicRecordName,
}

/// Runtime schema evolution mode.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SchemaEvolutionMode {
    /// Operator-managed evolution only.
    Manual,
    /// Automatically add backward-compatible nullable fields.
    #[default]
    AutoAdditive,
    /// Reserved for wider compatibility policy.
    Permissive,
}

/// Runtime behavior when a breaking schema change is observed.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum OnBreakingChange {
    /// Pause ingestion and mark the table degraded.
    #[default]
    Pause,
    /// Fail the process.
    Fail,
    /// Skip the offending message.
    SkipMessage,
}

/// Runtime schema evolution configuration.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct SchemaEvolutionRuntimeConfig {
    /// Evolution policy.
    #[serde(default)]
    pub mode: SchemaEvolutionMode,

    /// Behavior for breaking changes.
    #[serde(default)]
    pub on_breaking_change: OnBreakingChange,

    /// Minimum interval between schema update commits.
    #[serde(default = "default_schema_update_min_interval_seconds")]
    pub schema_update_min_interval_seconds: u64,
}

impl Default for SchemaEvolutionRuntimeConfig {
    fn default() -> Self {
        Self {
            mode: SchemaEvolutionMode::AutoAdditive,
            on_breaking_change: OnBreakingChange::Pause,
            schema_update_min_interval_seconds: default_schema_update_min_interval_seconds(),
        }
    }
}

/// Kafka auto offset reset strategy.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum OffsetReset {
    /// Start from earliest offset
    #[default]
    Earliest,
    /// Start from latest offset
    Latest,
}

/// Kafka security configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct KafkaSecurityConfig {
    /// Security protocol (PLAINTEXT, SSL, SASL_SSL, SASL_PLAINTEXT)
    pub protocol: Option<String>,

    /// SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
    pub sasl_mechanism: Option<String>,

    /// SASL username (plain string or `{ file = "path" }`)
    pub sasl_username: Option<Secret>,

    /// SASL password (plain string or `{ file = "path" }`)
    pub sasl_password: Option<Secret>,

    /// SSL CA certificate location
    pub ssl_ca_location: Option<PathBuf>,

    /// SSL client certificate location
    pub ssl_cert_location: Option<PathBuf>,

    /// SSL client key location
    pub ssl_key_location: Option<PathBuf>,
}

/// Iceberg table configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IcebergConfig {
    /// Catalog type
    pub catalog_type: CatalogType,

    /// Warehouse path (S3, GCS, or local path)
    pub warehouse_path: String,

    /// Database name
    pub database_name: String,

    /// Table name
    pub table_name: String,

    /// Target file size in MB
    #[serde(default = "default_target_file_size_mb")]
    pub target_file_size_mb: usize,

    /// Parquet compression
    #[serde(default)]
    pub compression: ParquetCompression,

    /// Partition specification
    #[serde(default)]
    pub partition_spec: Vec<PartitionField>,

    /// REST catalog URI (when catalog_type is Rest)
    pub rest_uri: Option<String>,

    /// Hive metastore URI (when catalog_type is Hive)
    pub hive_metastore_uri: Option<String>,

    /// AWS region (for Glue catalog and S3)
    pub aws_region: Option<String>,

    /// AWS access key ID (plain string or `{ file = "path" }`)
    pub aws_access_key_id: Option<Secret>,

    /// AWS secret access key (plain string or `{ file = "path" }`)
    pub aws_secret_access_key: Option<Secret>,

    /// S3 endpoint (for MinIO or other S3-compatible storage)
    pub s3_endpoint: Option<String>,

    /// Catalog manager configuration
    #[serde(default)]
    pub catalog_manager: CatalogManagerConfig,

    /// Table management configuration
    #[serde(default)]
    pub table_management: TableManagementConfig,

    /// REST catalog configuration (advanced)
    #[serde(default)]
    pub rest: RestCatalogConfig,

    /// Glue catalog configuration (advanced)
    #[serde(default)]
    pub glue: GlueCatalogConfig,

    /// Nessie catalog configuration (advanced)
    #[serde(default)]
    pub nessie: Option<NessieCatalogConfig>,

    /// SQL catalog configuration for local-first deployments.
    #[serde(default)]
    pub sql_catalog: Option<SqlCatalogConfig>,

    /// GCS bucket name override (default: parsed from warehouse_path)
    #[serde(default)]
    pub gcs_bucket_name: Option<String>,

    /// GCS service account key path (optional; falls through to ADC when unset)
    #[serde(default)]
    pub gcs_service_account_path: Option<String>,

    /// Azure container name override (default: parsed from warehouse_path)
    #[serde(default)]
    pub azure_container_name: Option<String>,

    /// Azure storage account name (required for Azure backends)
    #[serde(default)]
    pub azure_storage_account_name: Option<String>,

    /// Azure storage access key (optional; falls through to Managed Identity / env vars)
    #[serde(default)]
    pub azure_access_key: Option<Secret>,

    /// Object store configuration used by local-first deployments.
    #[serde(default)]
    pub object_store: ObjectStoreConfig,
}

/// Catalog manager configuration.
///
/// Controls connection pooling, health checks, and reconnection behavior.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CatalogManagerConfig {
    /// Connection pool size
    #[serde(default = "default_connection_pool_size")]
    pub connection_pool_size: usize,

    /// Health check interval in seconds
    #[serde(default = "default_health_check_interval_seconds")]
    pub health_check_interval_seconds: u64,

    /// Reconnect backoff intervals in milliseconds (exponential)
    #[serde(default = "default_reconnect_backoff_ms")]
    pub reconnect_backoff_ms: Vec<u64>,

    /// Maximum number of retries for transient failures
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Request timeout in seconds
    #[serde(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
}

impl Default for CatalogManagerConfig {
    fn default() -> Self {
        Self {
            connection_pool_size: default_connection_pool_size(),
            health_check_interval_seconds: default_health_check_interval_seconds(),
            reconnect_backoff_ms: default_reconnect_backoff_ms(),
            max_retries: default_max_retries(),
            request_timeout_seconds: default_request_timeout_seconds(),
        }
    }
}

/// Table management configuration.
///
/// Controls table creation, schema validation, and evolution behavior.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableManagementConfig {
    /// Automatically create table if it doesn't exist
    #[serde(default = "default_auto_create")]
    pub auto_create_if_missing: bool,

    /// Action when schema mismatch is detected
    #[serde(default)]
    pub schema_mismatch_action: SchemaMismatchAction,

    /// Partition strategy for new tables
    #[serde(default)]
    pub partition_strategy: PartitionStrategy,
}

impl Default for TableManagementConfig {
    fn default() -> Self {
        Self {
            auto_create_if_missing: default_auto_create(),
            schema_mismatch_action: SchemaMismatchAction::default(),
            partition_strategy: PartitionStrategy::default(),
        }
    }
}

/// Action to take when schema mismatch is detected.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SchemaMismatchAction {
    /// Fail the operation (default, safest)
    #[default]
    Fail,
    /// Evolve the schema automatically
    Evolve,
    /// Log a warning but continue
    Warn,
}

/// Partition strategy for table creation.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PartitionStrategy {
    /// Daily partitioning (default)
    #[default]
    Daily,
    /// Hourly partitioning
    Hourly,
    /// Identity partitioning (no transform)
    Identity,
    /// Bucket partitioning
    Bucket,
}

/// Credential type for REST catalog authentication.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CredentialType {
    /// No authentication
    #[default]
    None,
    /// Bearer token authentication
    Bearer,
    /// OAuth2 client credentials
    OAuth2,
}

/// REST catalog advanced configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RestCatalogConfig {
    /// Credential type (none, bearer, oauth2)
    #[serde(default)]
    pub credential_type: CredentialType,

    /// Credential value (token for bearer auth; plain string or `{ file = "path" }`)
    #[serde(default)]
    pub credential: Option<Secret>,

    /// OAuth2 token endpoint (for oauth2 credential type)
    #[serde(default)]
    pub oauth2_token_endpoint: Option<String>,

    /// OAuth2 client ID (plain string or `{ file = "path" }`)
    #[serde(default)]
    pub oauth2_client_id: Option<Secret>,

    /// OAuth2 client secret (plain string or `{ file = "path" }`)
    #[serde(default)]
    pub oauth2_client_secret: Option<Secret>,

    /// OAuth2 scope (optional)
    #[serde(default)]
    pub oauth2_scope: Option<String>,

    /// Request timeout in seconds
    #[serde(default)]
    pub request_timeout_seconds: Option<u64>,

    /// Custom headers to send with requests
    #[serde(default)]
    pub custom_headers: std::collections::HashMap<String, String>,
}

impl Default for RestCatalogConfig {
    fn default() -> Self {
        Self {
            credential_type: CredentialType::None,
            credential: None,
            oauth2_token_endpoint: None,
            oauth2_client_id: None,
            oauth2_client_secret: None,
            oauth2_scope: None,
            request_timeout_seconds: Some(30),
            custom_headers: std::collections::HashMap::new(),
        }
    }
}

/// AWS Glue catalog advanced configuration.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct GlueCatalogConfig {
    /// IAM role ARN to assume
    #[serde(default)]
    pub role_arn: Option<String>,

    /// External ID for role assumption
    #[serde(default)]
    pub external_id: Option<String>,

    /// Glue catalog ID (defaults to AWS account ID)
    #[serde(default)]
    pub catalog_id: Option<String>,
}

/// Nessie catalog advanced configuration.
///
/// Nessie provides Git-like versioned data lake management.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NessieCatalogConfig {
    /// Default branch/reference (defaults to "main")
    #[serde(default)]
    pub default_branch: Option<String>,

    /// API version (v1 or v2, defaults to "v1")
    #[serde(default)]
    pub api_version: Option<String>,
}

impl Default for NessieCatalogConfig {
    fn default() -> Self {
        Self {
            default_branch: Some("main".to_string()),
            api_version: Some("v1".to_string()),
        }
    }
}

/// SQL catalog backend type.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SqlCatalogBackend {
    /// SQLite file-backed catalog for local/edge deployments.
    #[default]
    Sqlite,
    /// PostgreSQL catalog for multi-writer deployments.
    Postgres,
}

/// SQL catalog configuration.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct SqlCatalogConfig {
    /// SQL backend type.
    #[serde(default)]
    pub r#type: SqlCatalogBackend,
    /// SQLx connection URL, e.g. sqlite:///var/lib/k2i/catalog.db.
    pub url: String,
    /// Catalog namespace/name inside the SQL catalog tables.
    #[serde(default = "default_sql_catalog_name")]
    pub catalog_name: String,
}

fn default_sql_catalog_name() -> String {
    "k2i".to_string()
}

/// Object store backend type.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ObjectStoreType {
    /// Local filesystem object store.
    #[default]
    Filesystem,
    /// S3-compatible object store.
    S3,
    /// Google Cloud Storage.
    Gcs,
    /// Azure Blob/Data Lake storage.
    Azure,
}

/// Object store configuration.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ObjectStoreConfig {
    /// Object store type.
    #[serde(default)]
    pub r#type: ObjectStoreType,
    /// Root URI/path for the object store.
    #[serde(default)]
    pub root: Option<String>,
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        Self {
            r#type: ObjectStoreType::Filesystem,
            root: None,
        }
    }
}

/// Iceberg catalog type.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CatalogType {
    /// REST catalog
    Rest,
    /// Hive metastore catalog
    Hive,
    /// AWS Glue catalog
    Glue,
    /// Nessie catalog
    Nessie,
    /// Embedded SQL catalog
    Sql,
}

/// Parquet compression codec.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ParquetCompression {
    /// Snappy compression (default, good balance)
    #[default]
    Snappy,
    /// Zstd compression (better ratio)
    Zstd,
    /// LZ4 compression (faster)
    Lz4,
    /// Gzip compression
    Gzip,
    /// No compression
    None,
}

/// Partition field specification.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PartitionField {
    /// Source field name
    pub source_field: String,

    /// Partition transform
    pub transform: PartitionTransform,
}

/// Iceberg partition transforms.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PartitionTransform {
    /// Identity transform
    Identity,
    /// Year transform
    Year,
    /// Month transform
    Month,
    /// Day transform
    Day,
    /// Hour transform
    Hour,
    /// Bucket transform
    #[serde(rename = "bucket")]
    Bucket {
        /// Number of buckets
        num_buckets: u32,
    },
    /// Truncate transform
    #[serde(rename = "truncate")]
    Truncate {
        /// Truncation width
        width: u32,
    },
}

/// Hot buffer configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BufferConfig {
    /// TTL in seconds (evict records after this time)
    #[serde(default = "default_ttl_seconds")]
    pub ttl_seconds: u64,

    /// Maximum buffer size in MB
    #[serde(default = "default_max_size_mb")]
    pub max_size_mb: usize,

    /// Flush interval in seconds
    #[serde(default = "default_flush_interval_seconds")]
    pub flush_interval_seconds: u64,

    /// Flush batch size (number of records)
    #[serde(default = "default_flush_batch_size")]
    pub flush_batch_size: usize,

    /// Memory alignment in bytes (for SIMD)
    #[serde(default = "default_memory_alignment")]
    pub memory_alignment_bytes: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            ttl_seconds: default_ttl_seconds(),
            max_size_mb: default_max_size_mb(),
            flush_interval_seconds: default_flush_interval_seconds(),
            flush_batch_size: default_flush_batch_size(),
            memory_alignment_bytes: default_memory_alignment(),
        }
    }
}

/// Transaction log configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TransactionLogConfig {
    /// Directory for transaction log files
    #[serde(default = "default_log_dir")]
    pub log_dir: PathBuf,

    /// Checkpoint interval (number of entries)
    #[serde(default = "default_checkpoint_interval_entries")]
    pub checkpoint_interval_entries: usize,

    /// Checkpoint interval in seconds
    #[serde(default = "default_checkpoint_interval_seconds")]
    pub checkpoint_interval_seconds: u64,

    /// Maximum number of log files to keep
    #[serde(default = "default_max_log_files")]
    pub max_log_files: usize,
}

impl Default for TransactionLogConfig {
    fn default() -> Self {
        Self {
            log_dir: default_log_dir(),
            checkpoint_interval_entries: default_checkpoint_interval_entries(),
            checkpoint_interval_seconds: default_checkpoint_interval_seconds(),
            max_log_files: default_max_log_files(),
        }
    }
}

/// Maintenance task configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MaintenanceConfig {
    /// Enable compaction
    #[serde(default = "default_enabled")]
    pub compaction_enabled: bool,

    /// Compaction interval in seconds
    #[serde(default = "default_compaction_interval")]
    pub compaction_interval_seconds: u64,

    /// Compaction threshold (files smaller than this are compacted)
    #[serde(default = "default_compaction_threshold_mb")]
    pub compaction_threshold_mb: usize,

    /// Compaction target file size
    #[serde(default = "default_compaction_target_mb")]
    pub compaction_target_mb: usize,

    /// Enable snapshot expiration
    #[serde(default = "default_enabled")]
    pub snapshot_expiration_enabled: bool,

    /// Snapshot retention in days
    #[serde(default = "default_snapshot_retention_days")]
    pub snapshot_retention_days: u32,

    /// Enable orphan cleanup
    #[serde(default = "default_enabled")]
    pub orphan_cleanup_enabled: bool,

    /// Orphan retention in days (safety period)
    #[serde(default = "default_orphan_retention_days")]
    pub orphan_retention_days: u32,

    /// Enable statistics update
    #[serde(default = "default_enabled")]
    pub statistics_enabled: bool,

    /// Statistics update interval in seconds
    #[serde(default = "default_statistics_interval")]
    pub statistics_interval_seconds: u64,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            compaction_enabled: default_enabled(),
            compaction_interval_seconds: default_compaction_interval(),
            compaction_threshold_mb: default_compaction_threshold_mb(),
            compaction_target_mb: default_compaction_target_mb(),
            snapshot_expiration_enabled: default_enabled(),
            snapshot_retention_days: default_snapshot_retention_days(),
            orphan_cleanup_enabled: default_enabled(),
            orphan_retention_days: default_orphan_retention_days(),
            statistics_enabled: default_enabled(),
            statistics_interval_seconds: default_statistics_interval(),
        }
    }
}

/// Monitoring configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitoringConfig {
    /// Metrics HTTP port
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,

    /// Health check HTTP port
    #[serde(default = "default_health_port")]
    pub health_port: u16,

    /// Log level
    #[serde(default)]
    pub log_level: LogLevel,

    /// Log format
    #[serde(default)]
    pub log_format: LogFormat,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            metrics_port: default_metrics_port(),
            health_port: default_health_port(),
            log_level: LogLevel::default(),
            log_format: LogFormat::default(),
        }
    }
}

/// Real-time read RPC configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcConfig {
    /// Enable the Unix socket read-state RPC server.
    #[serde(default)]
    pub enabled: bool,

    /// Unix socket path for local read clients.
    #[serde(default = "default_rpc_socket_path")]
    pub socket_path: PathBuf,

    /// Maximum time to wait for a requested LSN.
    #[serde(default = "default_rpc_read_timeout_ms")]
    pub read_timeout_ms: u64,

    /// Maximum number of concurrent scans to keep pinned.
    #[serde(default = "default_rpc_max_concurrent_scans")]
    pub max_concurrent_scans: usize,

    /// TTL for abandoned scans in seconds.
    #[serde(default = "default_rpc_scan_ttl_seconds")]
    pub scan_ttl_seconds: u64,

    /// Maximum accepted RPC frame size in bytes.
    #[serde(default = "default_rpc_max_frame_bytes")]
    pub max_frame_bytes: usize,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            socket_path: default_rpc_socket_path(),
            read_timeout_ms: default_rpc_read_timeout_ms(),
            max_concurrent_scans: default_rpc_max_concurrent_scans(),
            scan_ttl_seconds: default_rpc_scan_ttl_seconds(),
            max_frame_bytes: default_rpc_max_frame_bytes(),
        }
    }
}

/// Log level.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    /// Trace level
    Trace,
    /// Debug level
    Debug,
    /// Info level (default)
    #[default]
    Info,
    /// Warn level
    Warn,
    /// Error level
    Error,
}

/// Log format.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// JSON format (default)
    #[default]
    Json,
    /// Plain text format
    Text,
}

impl LogFormat {
    /// Parse a `K2I_MONITORING_LOG_FORMAT` value, case-insensitively.
    ///
    /// Returns `None` for unrecognized values so callers can warn and fall back.
    pub fn parse_env_value(value: &str) -> Option<Self> {
        match value.to_lowercase().as_str() {
            "json" => Some(Self::Json),
            "text" => Some(Self::Text),
            _ => None,
        }
    }

    /// Read the log format from `K2I_MONITORING_LOG_FORMAT`, if set and valid.
    ///
    /// Used to configure the tracing subscriber before the full config is
    /// loaded, so the env override applies to startup logging too.
    pub fn from_env() -> Option<Self> {
        Self::parse_env_value(&std::env::var("K2I_MONITORING_LOG_FORMAT").ok()?)
    }
}

// Default value functions
fn default_batch_size() -> usize {
    1000
}
fn default_batch_timeout_ms() -> u64 {
    5000
}
fn default_session_timeout_ms() -> u32 {
    30000
}
fn default_heartbeat_interval_ms() -> u32 {
    3000
}
fn default_max_poll_interval_ms() -> u32 {
    300000 // 5 minutes - must exceed longest flush time
}
fn default_target_file_size_mb() -> usize {
    512
}
fn default_ttl_seconds() -> u64 {
    60
}
fn default_max_size_mb() -> usize {
    500
}
fn default_flush_interval_seconds() -> u64 {
    30
}
fn default_flush_batch_size() -> usize {
    10000
}
fn default_memory_alignment() -> usize {
    64
}
fn default_true() -> bool {
    true
}
fn default_schema_registry_cache_ttl_seconds() -> u64 {
    300
}
fn default_schema_update_min_interval_seconds() -> u64 {
    60
}
fn default_log_dir() -> PathBuf {
    PathBuf::from("./transaction_logs")
}
fn default_checkpoint_interval_entries() -> usize {
    10000
}
fn default_checkpoint_interval_seconds() -> u64 {
    300
}
fn default_max_log_files() -> usize {
    10
}
fn default_enabled() -> bool {
    true
}
fn default_compaction_interval() -> u64 {
    3600
}
fn default_compaction_threshold_mb() -> usize {
    100
}
fn default_compaction_target_mb() -> usize {
    512
}
fn default_snapshot_retention_days() -> u32 {
    7
}
fn default_orphan_retention_days() -> u32 {
    3
}
fn default_statistics_interval() -> u64 {
    3600 // 1 hour
}
fn default_metrics_port() -> u16 {
    9090
}
fn default_health_port() -> u16 {
    8080
}
fn default_rpc_socket_path() -> PathBuf {
    PathBuf::from("./run/k2i.sock")
}
fn default_rpc_read_timeout_ms() -> u64 {
    1000
}
fn default_rpc_max_concurrent_scans() -> usize {
    64
}
fn default_rpc_scan_ttl_seconds() -> u64 {
    300
}
fn default_rpc_max_frame_bytes() -> usize {
    k2i_rpc::DEFAULT_MAX_FRAME_BYTES
}

// Catalog manager defaults
fn default_connection_pool_size() -> usize {
    5
}
fn default_health_check_interval_seconds() -> u64 {
    60
}
fn default_reconnect_backoff_ms() -> Vec<u64> {
    vec![100, 500, 2000, 5000]
}
fn default_max_retries() -> u32 {
    3
}
fn default_request_timeout_seconds() -> u64 {
    30
}

// Table management defaults
fn default_auto_create() -> bool {
    true
}

/// Extract the bucket from a cloud warehouse path, e.g. `s3://bucket/prefix`
/// with `scheme = "s3://"` yields `Some("bucket")`.
///
/// Returns `None` when the path does not use `scheme` or names no bucket.
/// Shared by [`Config::validate`] and the Iceberg writer so that what
/// validation accepts is exactly what the writer can build a store from.
pub(crate) fn bucket_from_warehouse(scheme: &str, warehouse_path: &str) -> Option<String> {
    let bucket = warehouse_path.strip_prefix(scheme)?.split('/').next()?;
    if bucket.is_empty() {
        None
    } else {
        Some(bucket.to_string())
    }
}

/// Extract the in-bucket prefix that follows the bucket in a cloud warehouse
/// path: `gs://bucket/some/prefix` yields `Some("some/prefix")`.
///
/// Returns `None` when the path has no subpath past the bucket, e.g. `s3://bucket`.
pub(crate) fn warehouse_prefix_after_bucket(scheme: &str, warehouse_path: &str) -> Option<String> {
    let after_bucket = warehouse_path.strip_prefix(scheme)?.split_once('/')?.1;
    let trimmed = after_bucket.trim_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Strip either Azure warehouse scheme, returning the remainder.
fn strip_azure_scheme(warehouse_path: &str) -> Option<&str> {
    warehouse_path
        .strip_prefix("az://")
        .or_else(|| warehouse_path.strip_prefix("abfs://"))
}

/// Extract the Azure container from a warehouse path, supporting both the
/// simple form `az://container/path` and the Hadoop ABFS form
/// `abfs://container@account.dfs.core.windows.net/path`.
pub(crate) fn azure_container_from_warehouse(warehouse_path: &str) -> Option<String> {
    let first_segment = strip_azure_scheme(warehouse_path)?.split('/').next()?;
    // ABFS form: take the segment before `@`. The simple form has no `@`.
    let container = first_segment.split('@').next()?;
    if container.is_empty() {
        None
    } else {
        Some(container.to_string())
    }
}

/// Extract the in-bucket prefix following the container in an Azure warehouse
/// path, for both the `az://` and `abfs://` forms.
pub(crate) fn azure_prefix_after_container(warehouse_path: &str) -> Option<String> {
    let after_container = strip_azure_scheme(warehouse_path)?.split_once('/')?.1;
    let trimmed = after_container.trim_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// `K2I_*` prefixes owned by tooling rather than by [`Config`].
///
/// These are consumed by the end-to-end test harness (`k2i-e2e-runner`) and its
/// helper subprocesses, which run with the same environment as the engine. They
/// are not config fields, so the typo warning below must not flag them.
const RESERVED_ENV_PREFIXES: &[&str] = &["K2I_E2E_", "K2I_PARQUET_", "K2I_ICEBERG_METADATA_PATH"];

/// Environment variables recognized by [`Config::apply_env_overrides`].
///
/// Keep in sync with the `env_val(...)` calls in that method.
const KNOWN_ENV_VARS: &[&str] = &[
    "K2I_KAFKA_BOOTSTRAP_SERVERS",
    "K2I_KAFKA_TOPIC",
    "K2I_KAFKA_CONSUMER_GROUP",
    "K2I_KAFKA_BATCH_SIZE",
    "K2I_KAFKA_BATCH_TIMEOUT_MS",
    "K2I_KAFKA_SESSION_TIMEOUT_MS",
    "K2I_KAFKA_HEARTBEAT_INTERVAL_MS",
    "K2I_KAFKA_MAX_POLL_INTERVAL_MS",
    "K2I_KAFKA_AUTO_OFFSET_RESET",
    "K2I_KAFKA_SECURITY_PROTOCOL",
    "K2I_KAFKA_SECURITY_SASL_MECHANISM",
    "K2I_KAFKA_SECURITY_SASL_USERNAME",
    "K2I_KAFKA_SECURITY_SASL_PASSWORD",
    "K2I_ICEBERG_CATALOG_TYPE",
    "K2I_ICEBERG_WAREHOUSE_PATH",
    "K2I_ICEBERG_DATABASE_NAME",
    "K2I_ICEBERG_TABLE_NAME",
    "K2I_ICEBERG_AWS_REGION",
    "K2I_ICEBERG_AWS_ACCESS_KEY_ID",
    "K2I_ICEBERG_AWS_SECRET_ACCESS_KEY",
    "K2I_ICEBERG_GCS_BUCKET_NAME",
    "K2I_ICEBERG_GCS_SERVICE_ACCOUNT_PATH",
    "K2I_ICEBERG_AZURE_STORAGE_ACCOUNT_NAME",
    "K2I_ICEBERG_AZURE_CONTAINER_NAME",
    "K2I_ICEBERG_AZURE_ACCESS_KEY",
    "K2I_ICEBERG_S3_ENDPOINT",
    "K2I_ICEBERG_REST_URI",
    "K2I_ICEBERG_HIVE_METASTORE_URI",
    "K2I_ICEBERG_REST_CREDENTIAL",
    "K2I_ICEBERG_REST_OAUTH2_CLIENT_ID",
    "K2I_ICEBERG_REST_OAUTH2_CLIENT_SECRET",
    "K2I_SCHEMA_EVOLUTION_MODE",
    "K2I_SCHEMA_EVOLUTION_ON_BREAKING_CHANGE",
    "K2I_BUFFER_TTL_SECONDS",
    "K2I_BUFFER_MAX_SIZE_MB",
    "K2I_BUFFER_FLUSH_INTERVAL_SECONDS",
    "K2I_TRANSACTION_LOG_LOG_DIR",
    "K2I_MONITORING_HEALTH_PORT",
    "K2I_MONITORING_METRICS_PORT",
    "K2I_MONITORING_LOG_LEVEL",
    "K2I_MONITORING_LOG_FORMAT",
    "K2I_RPC_ENABLED",
    "K2I_RPC_SOCKET_PATH",
];

impl Config {
    /// Load configuration from a TOML file.
    ///
    /// Applies the following in order:
    /// 1. Parse TOML values (secret `{ file = "..." }` refs resolve here)
    /// 2. Apply `K2I_*` environment variable overrides
    /// 3. Validate the merged configuration
    pub fn from_file(path: &std::path::Path) -> crate::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let mut config: Config = toml::from_str(&content)?;
        config.apply_env_overrides();
        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> crate::Result<()> {
        if self.kafka.bootstrap_servers.is_empty() {
            return Err(crate::Error::Config(
                "At least one bootstrap server required".into(),
            ));
        }

        if self.kafka.topic.is_empty() {
            return Err(crate::Error::Config("Kafka topic is required".into()));
        }

        if self.kafka.consumer_group.is_empty() {
            return Err(crate::Error::Config("Consumer group is required".into()));
        }

        if let KafkaFormatConfig::Protobuf(format) = &self.kafka.format {
            if format.schema_registry_url.trim().is_empty() {
                return Err(crate::Error::Config(
                    "kafka.format.schema_registry_url is required for protobuf".into(),
                ));
            }

            if format.cache_ttl_seconds == 0 {
                return Err(crate::Error::Config(
                    "kafka.format.cache_ttl_seconds must be greater than zero".into(),
                ));
            }

            if let Some(message_type) = &format.message_type {
                if message_type.trim().is_empty() {
                    return Err(crate::Error::Config(
                        "kafka.format.message_type must not be empty when set".into(),
                    ));
                }
            }

            if matches!(
                format.subject_strategy,
                ProtobufSubjectStrategy::RecordName | ProtobufSubjectStrategy::TopicRecordName
            ) && format.message_type.is_none()
            {
                return Err(crate::Error::Config(
                    "kafka.format.message_type is required for record_name and topic_record_name subject strategies".into(),
                ));
            }
        }

        if self.iceberg.warehouse_path.is_empty() {
            return Err(crate::Error::Config("Warehouse path is required".into()));
        }

        // Fail at startup rather than on the first flush: the object store is
        // not constructed until a writer is built, which for a long-running
        // ingest means a misconfigured warehouse surfaces minutes in, after the
        // process has already reported healthy.
        let warehouse = &self.iceberg.warehouse_path;
        if warehouse.starts_with("az://") || warehouse.starts_with("abfs://") {
            if self.iceberg.azure_storage_account_name.is_none() {
                return Err(crate::Error::Config(
                    "iceberg.azure_storage_account_name is required for az:// and abfs:// warehouse paths (it cannot be derived from the path)"
                        .into(),
                ));
            }
            if self.iceberg.azure_container_name.is_none()
                && azure_container_from_warehouse(warehouse).is_none()
            {
                return Err(crate::Error::Config(format!(
                    "could not determine the Azure container from warehouse path '{warehouse}'; set iceberg.azure_container_name"
                )));
            }
        } else if warehouse.starts_with("gs://")
            && self.iceberg.gcs_bucket_name.is_none()
            && bucket_from_warehouse("gs://", warehouse).is_none()
        {
            return Err(crate::Error::Config(format!(
                "could not determine the GCS bucket from warehouse path '{warehouse}'; set iceberg.gcs_bucket_name"
            )));
        } else if warehouse.starts_with("s3://")
            && bucket_from_warehouse("s3://", warehouse).is_none()
        {
            return Err(crate::Error::Config(format!(
                "could not determine the S3 bucket from warehouse path '{warehouse}'"
            )));
        }

        if self.iceberg.catalog_type == CatalogType::Sql {
            let sql_catalog = self.iceberg.sql_catalog.as_ref().ok_or_else(|| {
                crate::Error::Config("iceberg.sql_catalog is required when catalog_type=sql".into())
            })?;
            if sql_catalog.url.trim().is_empty() {
                return Err(crate::Error::Config(
                    "iceberg.sql_catalog.url must not be empty".into(),
                ));
            }
            if sql_catalog.catalog_name.trim().is_empty() {
                return Err(crate::Error::Config(
                    "iceberg.sql_catalog.catalog_name must not be empty".into(),
                ));
            }
        }

        if self.rpc.max_concurrent_scans == 0 {
            return Err(crate::Error::Config(
                "RPC max_concurrent_scans must be greater than zero".into(),
            ));
        }

        if self.rpc.max_frame_bytes == 0 {
            return Err(crate::Error::Config(
                "RPC max_frame_bytes must be greater than zero".into(),
            ));
        }

        if self.schema_evolution.schema_update_min_interval_seconds == 0 {
            return Err(crate::Error::Config(
                "schema_evolution.schema_update_min_interval_seconds must be greater than zero"
                    .into(),
            ));
        }

        if self.buffer.memory_alignment_bytes != 64 {
            tracing::warn!(
                alignment = self.buffer.memory_alignment_bytes,
                "Non-standard memory alignment may impact SIMD performance"
            );
        }

        Ok(())
    }

    /// Apply `K2I_*` environment variable overrides on top of TOML values.
    ///
    /// Env vars use the convention `K2I_` + uppercase field path with `_` separators.
    /// For example: `K2I_KAFKA_TOPIC`, `K2I_ICEBERG_WAREHOUSE_PATH`, `K2I_KAFKA_SECURITY_SASL_PASSWORD`.
    ///
    /// Invalid numeric/enum values are rejected with a warning (the TOML or
    /// default value is preserved). Unrecognized `K2I_*` variables are also
    /// logged so typos do not fail silently.
    fn apply_env_overrides(&mut self) {
        fn env_val(key: &str) -> Option<String> {
            std::env::var(key).ok()
        }

        fn parse_num<T: std::str::FromStr>(key: &str, v: &str) -> Option<T> {
            match v.parse::<T>() {
                Ok(n) => Some(n),
                Err(_) => {
                    tracing::warn!(var = key, value = %v, "Ignoring invalid numeric value");
                    None
                }
            }
        }

        fn warn_bad_enum(key: &str, v: &str, valid: &[&str]) {
            tracing::warn!(var = key, value = %v, valid = ?valid, "Ignoring invalid enum value");
        }

        // --- Kafka ---
        if let Some(v) = env_val("K2I_KAFKA_BOOTSTRAP_SERVERS") {
            self.kafka.bootstrap_servers = v.split(',').map(String::from).collect();
        }
        if let Some(v) = env_val("K2I_KAFKA_TOPIC") {
            self.kafka.topic = v;
        }
        if let Some(v) = env_val("K2I_KAFKA_CONSUMER_GROUP") {
            self.kafka.consumer_group = v;
        }
        if let Some(v) = env_val("K2I_KAFKA_BATCH_SIZE") {
            if let Some(n) = parse_num("K2I_KAFKA_BATCH_SIZE", &v) {
                self.kafka.batch_size = n;
            }
        }
        if let Some(v) = env_val("K2I_KAFKA_BATCH_TIMEOUT_MS") {
            if let Some(n) = parse_num("K2I_KAFKA_BATCH_TIMEOUT_MS", &v) {
                self.kafka.batch_timeout_ms = n;
            }
        }
        if let Some(v) = env_val("K2I_KAFKA_SESSION_TIMEOUT_MS") {
            if let Some(n) = parse_num("K2I_KAFKA_SESSION_TIMEOUT_MS", &v) {
                self.kafka.session_timeout_ms = n;
            }
        }
        if let Some(v) = env_val("K2I_KAFKA_HEARTBEAT_INTERVAL_MS") {
            if let Some(n) = parse_num("K2I_KAFKA_HEARTBEAT_INTERVAL_MS", &v) {
                self.kafka.heartbeat_interval_ms = n;
            }
        }
        if let Some(v) = env_val("K2I_KAFKA_MAX_POLL_INTERVAL_MS") {
            if let Some(n) = parse_num("K2I_KAFKA_MAX_POLL_INTERVAL_MS", &v) {
                self.kafka.max_poll_interval_ms = n;
            }
        }
        if let Some(v) = env_val("K2I_KAFKA_AUTO_OFFSET_RESET") {
            match v.to_lowercase().as_str() {
                "earliest" => self.kafka.auto_offset_reset = OffsetReset::Earliest,
                "latest" => self.kafka.auto_offset_reset = OffsetReset::Latest,
                _ => warn_bad_enum("K2I_KAFKA_AUTO_OFFSET_RESET", &v, &["earliest", "latest"]),
            }
        }

        // Kafka security
        if let Some(v) = env_val("K2I_KAFKA_SECURITY_PROTOCOL") {
            self.kafka.security.protocol = Some(v);
        }
        if let Some(v) = env_val("K2I_KAFKA_SECURITY_SASL_MECHANISM") {
            self.kafka.security.sasl_mechanism = Some(v);
        }
        if let Some(v) = env_val("K2I_KAFKA_SECURITY_SASL_USERNAME") {
            self.kafka.security.sasl_username = Some(Secret::new(v));
        }
        if let Some(v) = env_val("K2I_KAFKA_SECURITY_SASL_PASSWORD") {
            self.kafka.security.sasl_password = Some(Secret::new(v));
        }

        // --- Iceberg ---
        if let Some(v) = env_val("K2I_ICEBERG_CATALOG_TYPE") {
            match v.to_lowercase().as_str() {
                "rest" => self.iceberg.catalog_type = CatalogType::Rest,
                "glue" => self.iceberg.catalog_type = CatalogType::Glue,
                "hive" => self.iceberg.catalog_type = CatalogType::Hive,
                "nessie" => self.iceberg.catalog_type = CatalogType::Nessie,
                "sql" => self.iceberg.catalog_type = CatalogType::Sql,
                _ => warn_bad_enum(
                    "K2I_ICEBERG_CATALOG_TYPE",
                    &v,
                    &["rest", "glue", "hive", "nessie", "sql"],
                ),
            }
        }
        if let Some(v) = env_val("K2I_ICEBERG_WAREHOUSE_PATH") {
            self.iceberg.warehouse_path = v;
        }
        if let Some(v) = env_val("K2I_ICEBERG_DATABASE_NAME") {
            self.iceberg.database_name = v;
        }
        if let Some(v) = env_val("K2I_ICEBERG_TABLE_NAME") {
            self.iceberg.table_name = v;
        }
        if let Some(v) = env_val("K2I_ICEBERG_AWS_REGION") {
            self.iceberg.aws_region = Some(v);
        }
        if let Some(v) = env_val("K2I_ICEBERG_AWS_ACCESS_KEY_ID") {
            self.iceberg.aws_access_key_id = Some(Secret::new(v));
        }
        if let Some(v) = env_val("K2I_ICEBERG_AWS_SECRET_ACCESS_KEY") {
            self.iceberg.aws_secret_access_key = Some(Secret::new(v));
        }
        if let Some(v) = env_val("K2I_ICEBERG_GCS_BUCKET_NAME") {
            self.iceberg.gcs_bucket_name = Some(v);
        }
        if let Some(v) = env_val("K2I_ICEBERG_GCS_SERVICE_ACCOUNT_PATH") {
            self.iceberg.gcs_service_account_path = Some(v);
        }
        if let Some(v) = env_val("K2I_ICEBERG_AZURE_STORAGE_ACCOUNT_NAME") {
            self.iceberg.azure_storage_account_name = Some(v);
        }
        if let Some(v) = env_val("K2I_ICEBERG_AZURE_CONTAINER_NAME") {
            self.iceberg.azure_container_name = Some(v);
        }
        if let Some(v) = env_val("K2I_ICEBERG_AZURE_ACCESS_KEY") {
            self.iceberg.azure_access_key = Some(Secret::new(v));
        }
        if let Some(v) = env_val("K2I_ICEBERG_S3_ENDPOINT") {
            self.iceberg.s3_endpoint = Some(v);
        }
        if let Some(v) = env_val("K2I_ICEBERG_REST_URI") {
            self.iceberg.rest_uri = Some(v);
        }
        if let Some(v) = env_val("K2I_ICEBERG_HIVE_METASTORE_URI") {
            self.iceberg.hive_metastore_uri = Some(v);
        }

        // REST catalog advanced
        if let Some(v) = env_val("K2I_ICEBERG_REST_CREDENTIAL") {
            self.iceberg.rest.credential = Some(Secret::new(v));
        }
        if let Some(v) = env_val("K2I_ICEBERG_REST_OAUTH2_CLIENT_ID") {
            self.iceberg.rest.oauth2_client_id = Some(Secret::new(v));
        }
        if let Some(v) = env_val("K2I_ICEBERG_REST_OAUTH2_CLIENT_SECRET") {
            self.iceberg.rest.oauth2_client_secret = Some(Secret::new(v));
        }

        // --- Schema evolution ---
        if let Some(v) = env_val("K2I_SCHEMA_EVOLUTION_MODE") {
            match v.to_lowercase().as_str() {
                "manual" => self.schema_evolution.mode = SchemaEvolutionMode::Manual,
                "auto-additive" => self.schema_evolution.mode = SchemaEvolutionMode::AutoAdditive,
                "permissive" => self.schema_evolution.mode = SchemaEvolutionMode::Permissive,
                _ => warn_bad_enum(
                    "K2I_SCHEMA_EVOLUTION_MODE",
                    &v,
                    &["manual", "auto-additive", "permissive"],
                ),
            }
        }
        if let Some(v) = env_val("K2I_SCHEMA_EVOLUTION_ON_BREAKING_CHANGE") {
            match v.to_lowercase().as_str() {
                "pause" => self.schema_evolution.on_breaking_change = OnBreakingChange::Pause,
                "fail" => self.schema_evolution.on_breaking_change = OnBreakingChange::Fail,
                "skip-message" => {
                    self.schema_evolution.on_breaking_change = OnBreakingChange::SkipMessage
                }
                _ => warn_bad_enum(
                    "K2I_SCHEMA_EVOLUTION_ON_BREAKING_CHANGE",
                    &v,
                    &["pause", "fail", "skip-message"],
                ),
            }
        }

        // --- Buffer ---
        if let Some(v) = env_val("K2I_BUFFER_TTL_SECONDS") {
            if let Some(n) = parse_num("K2I_BUFFER_TTL_SECONDS", &v) {
                self.buffer.ttl_seconds = n;
            }
        }
        if let Some(v) = env_val("K2I_BUFFER_MAX_SIZE_MB") {
            if let Some(n) = parse_num("K2I_BUFFER_MAX_SIZE_MB", &v) {
                self.buffer.max_size_mb = n;
            }
        }
        if let Some(v) = env_val("K2I_BUFFER_FLUSH_INTERVAL_SECONDS") {
            if let Some(n) = parse_num("K2I_BUFFER_FLUSH_INTERVAL_SECONDS", &v) {
                self.buffer.flush_interval_seconds = n;
            }
        }

        // --- Transaction log ---
        if let Some(v) = env_val("K2I_TRANSACTION_LOG_LOG_DIR") {
            self.transaction_log.log_dir = std::path::PathBuf::from(v);
        }

        // --- Monitoring ---
        if let Some(v) = env_val("K2I_MONITORING_HEALTH_PORT") {
            if let Some(n) = parse_num("K2I_MONITORING_HEALTH_PORT", &v) {
                self.monitoring.health_port = n;
            }
        }
        if let Some(v) = env_val("K2I_MONITORING_METRICS_PORT") {
            if let Some(n) = parse_num("K2I_MONITORING_METRICS_PORT", &v) {
                self.monitoring.metrics_port = n;
            }
        }
        if let Some(v) = env_val("K2I_MONITORING_LOG_LEVEL") {
            match v.to_lowercase().as_str() {
                "trace" => self.monitoring.log_level = LogLevel::Trace,
                "debug" => self.monitoring.log_level = LogLevel::Debug,
                "info" => self.monitoring.log_level = LogLevel::Info,
                "warn" => self.monitoring.log_level = LogLevel::Warn,
                "error" => self.monitoring.log_level = LogLevel::Error,
                _ => warn_bad_enum(
                    "K2I_MONITORING_LOG_LEVEL",
                    &v,
                    &["trace", "debug", "info", "warn", "error"],
                ),
            }
        }
        if let Some(v) = env_val("K2I_MONITORING_LOG_FORMAT") {
            match LogFormat::parse_env_value(&v) {
                Some(format) => self.monitoring.log_format = format,
                None => warn_bad_enum("K2I_MONITORING_LOG_FORMAT", &v, &["json", "text"]),
            }
        }

        // --- RPC ---
        if let Some(v) = env_val("K2I_RPC_ENABLED") {
            // Anything unrecognized keeps the TOML value rather than silently
            // reading as `false` — `K2I_RPC_ENABLED=yes` must not disable RPC.
            match v.to_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => self.rpc.enabled = true,
                "false" | "0" | "no" | "off" => self.rpc.enabled = false,
                _ => warn_bad_enum(
                    "K2I_RPC_ENABLED",
                    &v,
                    &["true", "false", "1", "0", "yes", "no", "on", "off"],
                ),
            }
        }
        if let Some(v) = env_val("K2I_RPC_SOCKET_PATH") {
            self.rpc.socket_path = std::path::PathBuf::from(v);
        }

        // Warn on unrecognized K2I_* variables (typo detection).
        for (key, _) in std::env::vars_os() {
            let key = key.to_string_lossy();
            if key.starts_with("K2I_")
                && !KNOWN_ENV_VARS.contains(&key.as_ref())
                && !RESERVED_ENV_PREFIXES
                    .iter()
                    .any(|prefix| key.starts_with(prefix))
            {
                tracing::warn!(var = %key, "Unrecognized K2I_* environment variable ignored");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_buffer_config() {
        let config = BufferConfig::default();
        assert_eq!(config.ttl_seconds, 60);
        assert_eq!(config.max_size_mb, 500);
        assert_eq!(config.memory_alignment_bytes, 64);
    }

    #[test]
    fn test_config_validation() {
        let config = Config {
            kafka: KafkaConfig {
                bootstrap_servers: vec!["localhost:9092".into()],
                topic: "test".into(),
                consumer_group: "test-group".into(),
                batch_size: default_batch_size(),
                batch_timeout_ms: default_batch_timeout_ms(),
                session_timeout_ms: default_session_timeout_ms(),
                heartbeat_interval_ms: default_heartbeat_interval_ms(),
                max_poll_interval_ms: default_max_poll_interval_ms(),
                auto_offset_reset: OffsetReset::Earliest,
                security: KafkaSecurityConfig::default(),
                format: KafkaFormatConfig::Raw,
            },
            schema_evolution: SchemaEvolutionRuntimeConfig::default(),
            iceberg: IcebergConfig {
                catalog_type: CatalogType::Rest,
                warehouse_path: "s3://bucket/warehouse".into(),
                database_name: "db".into(),
                table_name: "events".into(),
                target_file_size_mb: default_target_file_size_mb(),
                compression: ParquetCompression::Snappy,
                partition_spec: vec![],
                rest_uri: Some("http://localhost:8181".into()),
                hive_metastore_uri: None,
                aws_region: None,
                aws_access_key_id: None,
                aws_secret_access_key: None,
                s3_endpoint: None,
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
            },
            buffer: BufferConfig::default(),
            transaction_log: TransactionLogConfig::default(),
            maintenance: MaintenanceConfig::default(),
            monitoring: MonitoringConfig::default(),
            rpc: RpcConfig::default(),
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_empty_servers() {
        let config = Config {
            kafka: KafkaConfig {
                bootstrap_servers: vec![],
                topic: "test".into(),
                consumer_group: "test-group".into(),
                batch_size: default_batch_size(),
                batch_timeout_ms: default_batch_timeout_ms(),
                session_timeout_ms: default_session_timeout_ms(),
                heartbeat_interval_ms: default_heartbeat_interval_ms(),
                max_poll_interval_ms: default_max_poll_interval_ms(),
                auto_offset_reset: OffsetReset::Earliest,
                security: KafkaSecurityConfig::default(),
                format: KafkaFormatConfig::Raw,
            },
            schema_evolution: SchemaEvolutionRuntimeConfig::default(),
            iceberg: IcebergConfig {
                catalog_type: CatalogType::Rest,
                warehouse_path: "s3://bucket/warehouse".into(),
                database_name: "db".into(),
                table_name: "events".into(),
                target_file_size_mb: default_target_file_size_mb(),
                compression: ParquetCompression::Snappy,
                partition_spec: vec![],
                rest_uri: None,
                hive_metastore_uri: None,
                aws_region: None,
                aws_access_key_id: None,
                aws_secret_access_key: None,
                s3_endpoint: None,
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
            },
            buffer: BufferConfig::default(),
            transaction_log: TransactionLogConfig::default(),
            maintenance: MaintenanceConfig::default(),
            monitoring: MonitoringConfig::default(),
            rpc: RpcConfig::default(),
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_default_transaction_log_config() {
        let config = TransactionLogConfig::default();
        assert_eq!(config.log_dir, PathBuf::from("./transaction_logs"));
        assert_eq!(config.checkpoint_interval_entries, 10000);
        assert_eq!(config.checkpoint_interval_seconds, 300);
        assert_eq!(config.max_log_files, 10);
    }

    #[test]
    fn test_default_maintenance_config() {
        let config = MaintenanceConfig::default();
        assert!(config.compaction_enabled);
        assert_eq!(config.compaction_interval_seconds, 3600);
        assert_eq!(config.compaction_threshold_mb, 100);
        assert_eq!(config.compaction_target_mb, 512);
        assert!(config.snapshot_expiration_enabled);
        assert_eq!(config.snapshot_retention_days, 7);
        assert!(config.orphan_cleanup_enabled);
        assert_eq!(config.orphan_retention_days, 3);
    }

    #[test]
    fn test_default_monitoring_config() {
        let config = MonitoringConfig::default();
        assert_eq!(config.metrics_port, 9090);
        assert_eq!(config.health_port, 8080);
        assert_eq!(config.log_level, LogLevel::Info);
        assert_eq!(config.log_format, LogFormat::Json);
    }

    #[test]
    fn test_default_rpc_config() {
        let config = RpcConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.socket_path, PathBuf::from("./run/k2i.sock"));
        assert_eq!(config.read_timeout_ms, 1000);
        assert_eq!(config.max_concurrent_scans, 64);
        assert_eq!(config.scan_ttl_seconds, 300);
        assert_eq!(config.max_frame_bytes, k2i_rpc::DEFAULT_MAX_FRAME_BYTES);
    }

    #[test]
    fn test_log_level_variants() {
        assert_eq!(LogLevel::default(), LogLevel::Info);
        assert_ne!(LogLevel::Trace, LogLevel::Debug);
        assert_ne!(LogLevel::Warn, LogLevel::Error);
    }

    #[test]
    fn test_log_format_variants() {
        assert_eq!(LogFormat::default(), LogFormat::Json);
        assert_ne!(LogFormat::Json, LogFormat::Text);
    }

    #[test]
    fn test_partition_transform_variants() {
        assert_eq!(PartitionTransform::Identity, PartitionTransform::Identity);
        assert_ne!(PartitionTransform::Year, PartitionTransform::Month);
        assert_ne!(PartitionTransform::Day, PartitionTransform::Hour);
    }

    #[test]
    fn test_offset_reset_variants() {
        assert_eq!(OffsetReset::default(), OffsetReset::Earliest);
        assert_ne!(OffsetReset::Earliest, OffsetReset::Latest);
    }

    #[test]
    fn test_catalog_type_variants() {
        // CatalogType doesn't have Default, just test equality
        assert_eq!(CatalogType::Rest, CatalogType::Rest);
        assert_ne!(CatalogType::Hive, CatalogType::Rest);
    }

    #[test]
    fn test_sql_catalog_config_validation() {
        let config: Config = toml::from_str(
            r#"
            [kafka]
            bootstrap_servers = ["localhost:9092"]
            topic = "test"
            consumer_group = "test"

            [iceberg]
            catalog_type = "sql"
            warehouse_path = "./data/warehouse"
            database_name = "f1"
            table_name = "historical"

            [iceberg.sql_catalog]
            type = "sqlite"
            url = "sqlite:///tmp/k2i-test-catalog.db"

            [iceberg.object_store]
            type = "filesystem"
            root = "./data/warehouse"
            "#,
        )
        .unwrap();

        assert_eq!(config.iceberg.catalog_type, CatalogType::Sql);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_parquet_compression_variants() {
        assert_eq!(ParquetCompression::default(), ParquetCompression::Snappy);
        assert_ne!(ParquetCompression::Zstd, ParquetCompression::Gzip);
        assert_ne!(ParquetCompression::Lz4, ParquetCompression::None);
    }

    #[test]
    fn test_default_kafka_security_config() {
        let config = KafkaSecurityConfig::default();
        assert!(config.protocol.is_none());
        assert!(config.ssl_ca_location.is_none());
        assert!(config.ssl_cert_location.is_none());
        assert!(config.ssl_key_location.is_none());
        assert!(config.sasl_mechanism.is_none());
        assert!(config.sasl_username.is_none());
        assert!(config.sasl_password.is_none());
    }

    #[test]
    fn test_config_validation_empty_topic() {
        let config = Config {
            kafka: KafkaConfig {
                bootstrap_servers: vec!["localhost:9092".into()],
                topic: "".into(), // Empty topic
                consumer_group: "test-group".into(),
                batch_size: default_batch_size(),
                batch_timeout_ms: default_batch_timeout_ms(),
                session_timeout_ms: default_session_timeout_ms(),
                heartbeat_interval_ms: default_heartbeat_interval_ms(),
                max_poll_interval_ms: default_max_poll_interval_ms(),
                auto_offset_reset: OffsetReset::Earliest,
                security: KafkaSecurityConfig::default(),
                format: KafkaFormatConfig::Raw,
            },
            schema_evolution: SchemaEvolutionRuntimeConfig::default(),
            iceberg: IcebergConfig {
                catalog_type: CatalogType::Rest,
                warehouse_path: "s3://bucket/warehouse".into(),
                database_name: "db".into(),
                table_name: "events".into(),
                target_file_size_mb: default_target_file_size_mb(),
                compression: ParquetCompression::Snappy,
                partition_spec: vec![],
                rest_uri: None,
                hive_metastore_uri: None,
                aws_region: None,
                aws_access_key_id: None,
                aws_secret_access_key: None,
                s3_endpoint: None,
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
            },
            buffer: BufferConfig::default(),
            transaction_log: TransactionLogConfig::default(),
            maintenance: MaintenanceConfig::default(),
            monitoring: MonitoringConfig::default(),
            rpc: RpcConfig::default(),
        };

        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("topic"));
    }

    #[test]
    fn test_config_validation_empty_consumer_group() {
        let config = Config {
            kafka: KafkaConfig {
                bootstrap_servers: vec!["localhost:9092".into()],
                topic: "test".into(),
                consumer_group: "".into(), // Empty consumer group
                batch_size: default_batch_size(),
                batch_timeout_ms: default_batch_timeout_ms(),
                session_timeout_ms: default_session_timeout_ms(),
                heartbeat_interval_ms: default_heartbeat_interval_ms(),
                max_poll_interval_ms: default_max_poll_interval_ms(),
                auto_offset_reset: OffsetReset::Earliest,
                security: KafkaSecurityConfig::default(),
                format: KafkaFormatConfig::Raw,
            },
            schema_evolution: SchemaEvolutionRuntimeConfig::default(),
            iceberg: IcebergConfig {
                catalog_type: CatalogType::Rest,
                warehouse_path: "s3://bucket/warehouse".into(),
                database_name: "db".into(),
                table_name: "events".into(),
                target_file_size_mb: default_target_file_size_mb(),
                compression: ParquetCompression::Snappy,
                partition_spec: vec![],
                rest_uri: None,
                hive_metastore_uri: None,
                aws_region: None,
                aws_access_key_id: None,
                aws_secret_access_key: None,
                s3_endpoint: None,
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
            },
            buffer: BufferConfig::default(),
            transaction_log: TransactionLogConfig::default(),
            maintenance: MaintenanceConfig::default(),
            monitoring: MonitoringConfig::default(),
            rpc: RpcConfig::default(),
        };

        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Consumer group"));
    }

    #[test]
    fn test_config_validation_empty_warehouse() {
        let config = Config {
            kafka: KafkaConfig {
                bootstrap_servers: vec!["localhost:9092".into()],
                topic: "test".into(),
                consumer_group: "test-group".into(),
                batch_size: default_batch_size(),
                batch_timeout_ms: default_batch_timeout_ms(),
                session_timeout_ms: default_session_timeout_ms(),
                heartbeat_interval_ms: default_heartbeat_interval_ms(),
                max_poll_interval_ms: default_max_poll_interval_ms(),
                auto_offset_reset: OffsetReset::Earliest,
                security: KafkaSecurityConfig::default(),
                format: KafkaFormatConfig::Raw,
            },
            schema_evolution: SchemaEvolutionRuntimeConfig::default(),
            iceberg: IcebergConfig {
                catalog_type: CatalogType::Rest,
                warehouse_path: "".into(), // Empty warehouse path
                database_name: "db".into(),
                table_name: "events".into(),
                target_file_size_mb: default_target_file_size_mb(),
                compression: ParquetCompression::Snappy,
                partition_spec: vec![],
                rest_uri: None,
                hive_metastore_uri: None,
                aws_region: None,
                aws_access_key_id: None,
                aws_secret_access_key: None,
                s3_endpoint: None,
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
            },
            buffer: BufferConfig::default(),
            transaction_log: TransactionLogConfig::default(),
            maintenance: MaintenanceConfig::default(),
            monitoring: MonitoringConfig::default(),
            rpc: RpcConfig::default(),
        };

        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Warehouse"));
    }

    #[test]
    fn test_kafka_security_protocol_strings() {
        // Security protocol is an Option<String>, test common values
        let config = KafkaSecurityConfig {
            protocol: Some("SASL_SSL".to_string()),
            sasl_mechanism: Some("SCRAM-SHA-256".to_string()),
            sasl_username: Some("user".into()),
            sasl_password: Some("pass".into()),
            ssl_ca_location: Some(PathBuf::from("/path/to/ca.pem")),
            ssl_cert_location: None,
            ssl_key_location: None,
        };

        assert_eq!(config.protocol, Some("SASL_SSL".to_string()));
        assert!(config.sasl_mechanism.is_some());
        assert!(config.ssl_ca_location.is_some());
    }

    #[test]
    fn test_default_catalog_manager_config() {
        let config = CatalogManagerConfig::default();
        assert_eq!(config.connection_pool_size, 5);
        assert_eq!(config.health_check_interval_seconds, 60);
        assert_eq!(config.reconnect_backoff_ms, vec![100, 500, 2000, 5000]);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.request_timeout_seconds, 30);
    }

    #[test]
    fn test_default_table_management_config() {
        let config = TableManagementConfig::default();
        assert!(config.auto_create_if_missing);
        assert_eq!(config.schema_mismatch_action, SchemaMismatchAction::Fail);
        assert_eq!(config.partition_strategy, PartitionStrategy::Daily);
    }

    #[test]
    fn test_schema_mismatch_action_variants() {
        assert_eq!(SchemaMismatchAction::default(), SchemaMismatchAction::Fail);
        assert_ne!(SchemaMismatchAction::Fail, SchemaMismatchAction::Evolve);
        assert_ne!(SchemaMismatchAction::Evolve, SchemaMismatchAction::Warn);
    }

    #[test]
    fn test_partition_strategy_variants() {
        assert_eq!(PartitionStrategy::default(), PartitionStrategy::Daily);
        assert_ne!(PartitionStrategy::Daily, PartitionStrategy::Hourly);
        assert_ne!(PartitionStrategy::Identity, PartitionStrategy::Bucket);
    }

    #[test]
    fn test_default_rest_catalog_config() {
        let config = RestCatalogConfig::default();
        assert_eq!(config.credential_type, CredentialType::None);
        assert!(config.credential.is_none());
        assert!(config.oauth2_token_endpoint.is_none());
        assert!(config.oauth2_client_id.is_none());
        assert!(config.oauth2_client_secret.is_none());
        assert!(config.oauth2_scope.is_none());
        assert_eq!(config.request_timeout_seconds, Some(30));
        assert!(config.custom_headers.is_empty());
    }

    #[test]
    fn test_default_glue_catalog_config() {
        let config = GlueCatalogConfig::default();
        assert!(config.role_arn.is_none());
        assert!(config.external_id.is_none());
        assert!(config.catalog_id.is_none());
    }

    #[test]
    fn test_rest_catalog_config_with_values() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("X-Custom-Header".to_string(), "value".to_string());

        let config = RestCatalogConfig {
            credential_type: CredentialType::Bearer,
            credential: Some("token123".into()),
            oauth2_token_endpoint: None,
            oauth2_client_id: None,
            oauth2_client_secret: None,
            oauth2_scope: None,
            request_timeout_seconds: Some(60),
            custom_headers: headers,
        };

        assert_eq!(config.credential_type, CredentialType::Bearer);
        assert_eq!(config.credential.as_deref(), Some("token123"));
        assert_eq!(config.request_timeout_seconds, Some(60));
        assert_eq!(config.custom_headers.len(), 1);
    }

    #[test]
    fn test_glue_catalog_config_with_values() {
        let config = GlueCatalogConfig {
            role_arn: Some("arn:aws:iam::123456789012:role/MyRole".to_string()),
            external_id: Some("external-123".to_string()),
            catalog_id: Some("123456789012".to_string()),
        };

        assert!(config.role_arn.is_some());
        assert!(config.external_id.is_some());
        assert!(config.catalog_id.is_some());
    }
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_secret_file_ref_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let pass_path = dir.path().join("kafka-password");
        std::fs::write(&pass_path, "hunter2\n").unwrap();

        let toml = format!(
            r#"
[kafka]
bootstrap_servers = ["localhost:9092"]
topic = "events"
consumer_group = "k2i"

[kafka.security]
protocol = "SASL_SSL"
sasl_password = {{ file = "{}" }}

[iceberg]
catalog_type = "sql"
warehouse_path = "/tmp/warehouse"
database_name = "db"
table_name = "tbl"
"#,
            pass_path.display()
        );

        let config: Config = toml::from_str(&toml).unwrap();
        assert_eq!(
            config.kafka.security.sasl_password.as_deref(),
            Some("hunter2")
        );
    }

    #[test]
    fn test_secret_file_ref_missing_file_errors() {
        let toml = r#"
[kafka]
bootstrap_servers = ["localhost:9092"]
topic = "events"
consumer_group = "k2i"

[kafka.security]
sasl_password = { file = "/nonexistent/secret/path" }

[iceberg]
catalog_type = "sql"
warehouse_path = "/tmp/warehouse"
database_name = "db"
table_name = "tbl"
"#;

        let err = toml::from_str::<Config>(toml).unwrap_err();
        assert!(err.to_string().contains("failed to read secret file"));
    }

    #[test]
    fn test_secret_plain_string_still_works() {
        let toml = r#"
[kafka]
bootstrap_servers = ["localhost:9092"]
topic = "events"
consumer_group = "k2i"

[kafka.security]
sasl_password = "hunter2"

[iceberg]
catalog_type = "sql"
warehouse_path = "/tmp/warehouse"
database_name = "db"
table_name = "tbl"
"#;

        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(
            config.kafka.security.sasl_password.as_deref(),
            Some("hunter2")
        );
    }

    #[test]
    fn test_secret_debug_is_redacted() {
        let config = KafkaSecurityConfig {
            sasl_password: Some(Secret::new("hunter2")),
            ..KafkaSecurityConfig::default()
        };
        let debug = format!("{:?}", config);
        assert!(!debug.contains("hunter2"));
        assert!(debug.contains("REDACTED"));
    }

    #[test]
    fn test_env_override_string_field() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("K2I_KAFKA_TOPIC", "env-topic");

        let mut config = test_config();
        config.kafka.topic = "toml-topic".into();

        config.apply_env_overrides();

        assert_eq!(config.kafka.topic, "env-topic");

        std::env::remove_var("K2I_KAFKA_TOPIC");
    }

    #[test]
    fn test_env_override_numeric_field() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("K2I_KAFKA_BATCH_SIZE", "500");

        let mut config = test_config();
        config.apply_env_overrides();

        assert_eq!(config.kafka.batch_size, 500);

        std::env::remove_var("K2I_KAFKA_BATCH_SIZE");
    }

    #[test]
    fn test_env_override_iceberg_enum() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("K2I_ICEBERG_CATALOG_TYPE", "nessie");

        let mut config = test_config();
        config.apply_env_overrides();

        assert_eq!(config.iceberg.catalog_type, CatalogType::Nessie);

        std::env::remove_var("K2I_ICEBERG_CATALOG_TYPE");
    }

    #[test]
    fn test_env_override_invalid_numeric_ignored() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("K2I_KAFKA_BATCH_SIZE", "not-a-number");

        let mut config = test_config();
        config.kafka.batch_size = 1000;
        config.apply_env_overrides();

        // Invalid parse should leave the original value intact
        assert_eq!(config.kafka.batch_size, 1000);

        std::env::remove_var("K2I_KAFKA_BATCH_SIZE");
    }

    #[test]
    fn test_env_override_secret_field() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("K2I_KAFKA_SECURITY_SASL_PASSWORD", "env-secret");

        let mut config = test_config();
        config.apply_env_overrides();

        assert_eq!(
            config.kafka.security.sasl_password.as_deref(),
            Some("env-secret")
        );

        std::env::remove_var("K2I_KAFKA_SECURITY_SASL_PASSWORD");
    }

    /// `azure_storage_account_name` is mandatory for Azure backends, so a
    /// Kubernetes deployment configuring purely through env must be able to set
    /// it. Same for the other cloud-store fields.
    #[test]
    fn test_env_override_cloud_store_fields() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("K2I_ICEBERG_GCS_BUCKET_NAME", "env-bucket");
        std::env::set_var("K2I_ICEBERG_GCS_SERVICE_ACCOUNT_PATH", "/run/gcp/key.json");
        std::env::set_var("K2I_ICEBERG_AZURE_STORAGE_ACCOUNT_NAME", "envacct");
        std::env::set_var("K2I_ICEBERG_AZURE_CONTAINER_NAME", "env-container");
        std::env::set_var("K2I_ICEBERG_AZURE_ACCESS_KEY", "env-azure-key");

        let mut config = test_config();
        config.apply_env_overrides();

        assert_eq!(
            config.iceberg.gcs_bucket_name.as_deref(),
            Some("env-bucket")
        );
        assert_eq!(
            config.iceberg.gcs_service_account_path.as_deref(),
            Some("/run/gcp/key.json")
        );
        assert_eq!(
            config.iceberg.azure_storage_account_name.as_deref(),
            Some("envacct")
        );
        assert_eq!(
            config.iceberg.azure_container_name.as_deref(),
            Some("env-container")
        );
        assert_eq!(
            config.iceberg.azure_access_key.as_deref(),
            Some("env-azure-key")
        );

        for var in [
            "K2I_ICEBERG_GCS_BUCKET_NAME",
            "K2I_ICEBERG_GCS_SERVICE_ACCOUNT_PATH",
            "K2I_ICEBERG_AZURE_STORAGE_ACCOUNT_NAME",
            "K2I_ICEBERG_AZURE_CONTAINER_NAME",
            "K2I_ICEBERG_AZURE_ACCESS_KEY",
        ] {
            std::env::remove_var(var);
        }
    }

    /// An unparseable boolean must not read as `false` — that would silently
    /// disable RPC for a deployment that set `K2I_RPC_ENABLED=yes`.
    #[test]
    fn test_env_override_rpc_enabled_booleans() {
        let _guard = ENV_LOCK.lock().unwrap();

        for (value, expected) in [
            ("true", true),
            ("1", true),
            ("yes", true),
            ("ON", true),
            ("false", false),
            ("0", false),
            ("no", false),
        ] {
            std::env::set_var("K2I_RPC_ENABLED", value);
            let mut config = test_config();
            config.rpc.enabled = !expected;
            config.apply_env_overrides();
            assert_eq!(config.rpc.enabled, expected, "K2I_RPC_ENABLED={value}");
        }

        // Garbage preserves the configured value in both directions.
        std::env::set_var("K2I_RPC_ENABLED", "maybe");
        for configured in [true, false] {
            let mut config = test_config();
            config.rpc.enabled = configured;
            config.apply_env_overrides();
            assert_eq!(config.rpc.enabled, configured);
        }

        std::env::remove_var("K2I_RPC_ENABLED");
    }

    /// `Config` derives `Serialize`. Anything that dumps or echoes it — a
    /// diagnostic bundle, an RPC response, a future `config dump` — must not be
    /// able to emit a credential in the clear.
    #[test]
    fn test_serializing_config_never_emits_a_plaintext_secret() {
        let mut config = test_config();
        config.kafka.security.sasl_username = Some(Secret::new("svc-account"));
        config.kafka.security.sasl_password = Some(Secret::new("hunter2"));
        config.iceberg.aws_access_key_id = Some(Secret::new("AKIAPLAINTEXT"));
        config.iceberg.aws_secret_access_key = Some(Secret::new("aws-secret-value"));
        config.iceberg.azure_access_key = Some(Secret::new("azure-secret-value"));
        config.iceberg.rest.credential = Some(Secret::new("bearer-token-value"));
        config.iceberg.rest.oauth2_client_id = Some(Secret::new("oauth-client-id"));
        config.iceberg.rest.oauth2_client_secret = Some(Secret::new("oauth-client-secret"));

        let json = serde_json::to_string(&config).expect("Config should serialize");
        let toml_out = toml::to_string(&config).expect("Config should serialize to TOML");
        let debug = format!("{config:?}");

        for secret in [
            "svc-account",
            "hunter2",
            "AKIAPLAINTEXT",
            "aws-secret-value",
            "azure-secret-value",
            "bearer-token-value",
            "oauth-client-id",
            "oauth-client-secret",
        ] {
            assert!(!json.contains(secret), "JSON leaked {secret}");
            assert!(!toml_out.contains(secret), "TOML leaked {secret}");
            assert!(!debug.contains(secret), "Debug leaked {secret}");
        }

        assert!(json.contains(Secret::REDACTED));
        assert!(debug.contains(Secret::REDACTED));
    }

    /// `expose()` remains the single deliberate way to read the value, so the
    /// redacted output above does not come at the cost of actually using it.
    #[test]
    fn test_expose_still_returns_the_plaintext() {
        let secret = Secret::new("hunter2");
        assert_eq!(secret.expose(), "hunter2");
        assert_eq!(&*secret, "hunter2");
    }

    #[test]
    fn test_bucket_from_warehouse() {
        assert_eq!(
            bucket_from_warehouse("s3://", "s3://my-bucket/warehouse"),
            Some("my-bucket".to_string())
        );
        // Bucket with no subpath
        assert_eq!(
            bucket_from_warehouse("s3://", "s3://my-bucket"),
            Some("my-bucket".to_string())
        );
        // Wrong scheme
        assert_eq!(
            bucket_from_warehouse("s3://", "gs://bucket/warehouse"),
            None
        );
        // Missing bucket
        assert_eq!(bucket_from_warehouse("s3://", "s3:///warehouse"), None);
        assert_eq!(bucket_from_warehouse("s3://", "s3://"), None);
    }

    #[test]
    fn test_warehouse_prefix_after_bucket_strips_scheme_and_bucket() {
        // Bucket-only path: no prefix
        assert_eq!(
            warehouse_prefix_after_bucket("s3://", "s3://my-bucket"),
            None
        );
        // Trailing slash normalizes to None
        assert_eq!(
            warehouse_prefix_after_bucket("s3://", "s3://my-bucket/"),
            None
        );
        // Single-segment prefix
        assert_eq!(
            warehouse_prefix_after_bucket("s3://", "s3://my-bucket/warehouse"),
            Some("warehouse".to_string())
        );
        // Multi-segment prefix preserved
        assert_eq!(
            warehouse_prefix_after_bucket("gs://", "gs://bucket/warehouse/prod"),
            Some("warehouse/prod".to_string())
        );
        // Wrong scheme returns None
        assert_eq!(
            warehouse_prefix_after_bucket("s3://", "gs://bucket/warehouse"),
            None
        );
    }

    #[test]
    fn test_azure_container_from_warehouse_both_url_forms() {
        // Simple form: az://container/path
        assert_eq!(
            azure_container_from_warehouse("az://my-container/warehouse"),
            Some("my-container".to_string())
        );
        // Container-only (no subpath)
        assert_eq!(
            azure_container_from_warehouse("az://my-container"),
            Some("my-container".to_string())
        );
        // Hadoop ABFS form must extract the container BEFORE the `@`,
        // not the whole `container@account.dfs.core.windows.net` segment.
        assert_eq!(
            azure_container_from_warehouse("abfs://container@account.dfs.core.windows.net/path"),
            Some("container".to_string())
        );
        // ABFS with multi-segment path
        assert_eq!(
            azure_container_from_warehouse("abfs://events@prodacct/warehouse/raw"),
            Some("events".to_string())
        );
        // Not an azure scheme
        assert_eq!(azure_container_from_warehouse("s3://bucket/path"), None);
        // Empty container before `@`
        assert_eq!(azure_container_from_warehouse("az://@account/path"), None);
    }

    #[test]
    fn test_azure_prefix_after_container() {
        // No subpath past container
        assert_eq!(azure_prefix_after_container("az://container"), None);
        // Simple form subpath
        assert_eq!(
            azure_prefix_after_container("az://container/warehouse"),
            Some("warehouse".to_string())
        );
        // ABFS form: prefix is the part AFTER the first `/`, unaffected by `@account`
        assert_eq!(
            azure_prefix_after_container(
                "abfs://container@account.dfs.core.windows.net/warehouse/raw"
            ),
            Some("warehouse/raw".to_string())
        );
        // Wrong scheme
        assert_eq!(azure_prefix_after_container("gs://bucket/warehouse"), None);
    }

    /// `test_config()` intentionally uses a SQL catalog without its settings, so
    /// it fails `validate()` for reasons unrelated to the warehouse path. These
    /// tests need a baseline that otherwise passes.
    fn validatable_config() -> Config {
        let mut config = test_config();
        config.iceberg.catalog_type = CatalogType::Rest;
        config.iceberg.sql_catalog = None;
        config
    }

    /// Azure needs a storage account name that cannot be derived from either
    /// URL form. Catching it in `validate()` means a misconfigured deployment
    /// fails at startup instead of on the first flush, minutes after the
    /// process has already reported healthy.
    #[test]
    fn test_validate_requires_azure_storage_account_name() {
        let mut config = validatable_config();
        config.iceberg.warehouse_path = "az://my-container/warehouse".into();

        let err = config
            .validate()
            .expect_err("Azure warehouse without an account name must fail validation");
        assert!(
            err.to_string().contains("azure_storage_account_name"),
            "error should name the missing field, got: {err}"
        );

        config.iceberg.azure_storage_account_name = Some("myacct".into());
        config.validate().expect("should validate once set");
    }

    #[test]
    fn test_validate_accepts_abfs_form_with_account_name() {
        let mut config = validatable_config();
        config.iceberg.warehouse_path = "abfs://cont@myacct.dfs.core.windows.net/warehouse".into();
        config.iceberg.azure_storage_account_name = Some("myacct".into());
        config.validate().expect("ABFS form should validate");
    }

    #[test]
    fn test_validate_rejects_cloud_warehouse_without_a_bucket() {
        for path in ["s3://", "gs://", "s3:///warehouse"] {
            let mut config = validatable_config();
            config.iceberg.warehouse_path = path.into();
            assert!(
                config.validate().is_err(),
                "{path} names no bucket and should fail validation"
            );
        }
    }

    #[test]
    fn test_validate_accepts_ordinary_cloud_and_local_warehouses() {
        for path in [
            "s3://bucket/warehouse",
            "s3://bucket",
            "gs://bucket/warehouse/prod",
            "/tmp/warehouse",
            "file:///tmp/warehouse",
        ] {
            let mut config = validatable_config();
            config.iceberg.warehouse_path = path.into();
            config
                .validate()
                .unwrap_or_else(|e| panic!("{path} should validate, got: {e}"));
        }
    }

    #[test]
    fn test_log_format_parse_env_value() {
        assert_eq!(LogFormat::parse_env_value("TEXT"), Some(LogFormat::Text));
        assert_eq!(LogFormat::parse_env_value("json"), Some(LogFormat::Json));
        assert_eq!(LogFormat::parse_env_value("yaml"), None);
    }

    /// Values are never shell-interpolated. `docs/configuration.md` once
    /// promised `${VAR}` substitution; following it would have authenticated
    /// with the literal string. Pin the real behaviour so the documented
    /// mechanisms (`{ file = ... }` and `K2I_*`) stay the only two.
    #[test]
    fn test_secret_values_are_not_shell_interpolated() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("SOME_KAFKA_PASSWORD", "real-secret");

        let toml = r#"
[kafka]
bootstrap_servers = ["localhost:9092"]
topic = "events"
consumer_group = "k2i"

[kafka.security]
sasl_password = "${SOME_KAFKA_PASSWORD}"

[iceberg]
catalog_type = "rest"
warehouse_path = "/tmp/warehouse"
database_name = "db"
table_name = "tbl"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(
            config.kafka.security.sasl_password.as_deref(),
            Some("${SOME_KAFKA_PASSWORD}"),
            "`${{VAR}}` is stored literally; it is not an interpolation syntax"
        );

        std::env::remove_var("SOME_KAFKA_PASSWORD");
    }

    /// The env var table in `docs/kubernetes.md` is the deployment contract.
    /// If a variable is added to `KNOWN_ENV_VARS` without documenting it,
    /// operators cannot discover it — and an undocumented name silently does
    /// nothing when mistyped.
    #[test]
    fn test_known_env_vars_are_documented() {
        let docs = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/kubernetes.md"
        ))
        .expect("docs/kubernetes.md should be readable");

        let undocumented: Vec<_> = KNOWN_ENV_VARS
            .iter()
            .filter(|var| !docs.contains(&format!("`{var}`")))
            .collect();

        assert!(
            undocumented.is_empty(),
            "these K2I_* variables are not documented in docs/kubernetes.md: {undocumented:?}"
        );
    }

    #[test]
    fn test_known_env_vars_have_no_duplicates() {
        let unique: std::collections::HashSet<_> = KNOWN_ENV_VARS.iter().collect();
        assert_eq!(unique.len(), KNOWN_ENV_VARS.len());
    }

    /// Harness variables share the engine's environment during e2e runs; they
    /// must not be reported as typos.
    #[test]
    fn test_reserved_env_prefixes_cover_harness_variables() {
        for var in [
            "K2I_E2E_TOPIC",
            "K2I_E2E_WAREHOUSE",
            "K2I_PARQUET_PATHS",
            "K2I_ICEBERG_METADATA_PATH",
        ] {
            assert!(
                RESERVED_ENV_PREFIXES.iter().any(|p| var.starts_with(p)),
                "{var} should be treated as a reserved harness variable"
            );
        }
        // A genuine typo is still not reserved.
        assert!(!RESERVED_ENV_PREFIXES
            .iter()
            .any(|p| "K2I_KAFKA_TOPC".starts_with(p)));
    }

    /// Build a minimal valid Config for override tests.
    fn test_config() -> Config {
        Config {
            kafka: KafkaConfig {
                bootstrap_servers: vec!["localhost:9092".into()],
                topic: "test".into(),
                consumer_group: "test-group".into(),
                batch_size: default_batch_size(),
                batch_timeout_ms: default_batch_timeout_ms(),
                session_timeout_ms: default_session_timeout_ms(),
                heartbeat_interval_ms: default_heartbeat_interval_ms(),
                max_poll_interval_ms: default_max_poll_interval_ms(),
                auto_offset_reset: OffsetReset::Earliest,
                security: KafkaSecurityConfig::default(),
                format: KafkaFormatConfig::Raw,
            },
            schema_evolution: SchemaEvolutionRuntimeConfig::default(),
            iceberg: IcebergConfig {
                catalog_type: CatalogType::Sql,
                warehouse_path: "/tmp/warehouse".into(),
                database_name: "db".into(),
                table_name: "tbl".into(),
                target_file_size_mb: default_target_file_size_mb(),
                compression: ParquetCompression::Snappy,
                partition_spec: vec![],
                rest_uri: None,
                hive_metastore_uri: None,
                aws_region: None,
                aws_access_key_id: None,
                aws_secret_access_key: None,
                s3_endpoint: None,
                catalog_manager: CatalogManagerConfig::default(),
                table_management: TableManagementConfig::default(),
                rest: RestCatalogConfig::default(),
                glue: GlueCatalogConfig::default(),
                nessie: None,
                sql_catalog: None,
                gcs_bucket_name: None,
                gcs_service_account_path: None,
                azure_container_name: None,
                azure_storage_account_name: None,
                azure_access_key: None,
                object_store: ObjectStoreConfig::default(),
            },
            buffer: BufferConfig::default(),
            transaction_log: TransactionLogConfig::default(),
            maintenance: MaintenanceConfig::default(),
            monitoring: MonitoringConfig::default(),
            rpc: RpcConfig::default(),
        }
    }
}
