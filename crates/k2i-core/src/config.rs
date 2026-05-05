//! Configuration structures for k2i.
//!
//! Configuration is loaded from TOML files and can be overridden via CLI flags.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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

    /// SASL username
    pub sasl_username: Option<String>,

    /// SASL password
    pub sasl_password: Option<String>,

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

    /// AWS access key ID
    pub aws_access_key_id: Option<String>,

    /// AWS secret access key
    pub aws_secret_access_key: Option<String>,

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

    /// Credential value (token for bearer auth)
    #[serde(default)]
    pub credential: Option<String>,

    /// OAuth2 token endpoint (for oauth2 credential type)
    #[serde(default)]
    pub oauth2_token_endpoint: Option<String>,

    /// OAuth2 client ID
    #[serde(default)]
    pub oauth2_client_id: Option<String>,

    /// OAuth2 client secret
    #[serde(default)]
    pub oauth2_client_secret: Option<String>,

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

impl Config {
    /// Load configuration from a TOML file.
    pub fn from_file(path: &std::path::Path) -> crate::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
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
            sasl_username: Some("user".to_string()),
            sasl_password: Some("pass".to_string()),
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
            credential: Some("token123".to_string()),
            oauth2_token_endpoint: None,
            oauth2_client_id: None,
            oauth2_client_secret: None,
            oauth2_scope: None,
            request_timeout_seconds: Some(60),
            custom_headers: headers,
        };

        assert_eq!(config.credential_type, CredentialType::Bearer);
        assert_eq!(config.credential, Some("token123".to_string()));
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
}
