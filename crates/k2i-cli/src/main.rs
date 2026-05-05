//! K2I CLI - Kafka to Iceberg streaming ingestion tool.

use anyhow::Result;
use clap::{Parser, Subcommand};
use k2i_core::config::LogFormat;
use k2i_core::Config;
use std::path::PathBuf;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Exit codes for CLI operations.
///
/// Following Unix conventions:
/// - 0: Success
/// - 1-127: Application errors
/// - 128+N: Signal N received (e.g., 130 = SIGINT)
#[repr(i32)]
#[derive(Debug, Clone, Copy)]
pub enum ExitCode {
    /// Successful execution
    Success = 0,
    /// Configuration error (invalid config file, missing required fields)
    ConfigError = 1,
    /// Kafka-related error (connection, consumer, producer)
    KafkaError = 2,
    /// Iceberg-related error (catalog, commit, schema)
    IcebergError = 3,
    /// Storage error (S3, GCS, Azure, filesystem)
    StorageError = 4,
    /// Transaction log error
    TransactionLogError = 5,
    /// Health check failure
    HealthCheckError = 6,
    /// General runtime error
    RuntimeError = 10,
    /// Signal interrupt (SIGINT = 2, so 128 + 2 = 130)
    SignalInterrupt = 130,
}

impl ExitCode {
    /// Convert an error to an exit code by inspecting the error message.
    fn from_error(error: &anyhow::Error) -> Self {
        let error_str = error.to_string().to_lowercase();

        if error_str.contains("config") || error_str.contains("toml") || error_str.contains("parse")
        {
            ExitCode::ConfigError
        } else if error_str.contains("kafka")
            || error_str.contains("rdkafka")
            || error_str.contains("consumer")
        {
            ExitCode::KafkaError
        } else if error_str.contains("iceberg")
            || error_str.contains("catalog")
            || error_str.contains("snapshot")
        {
            ExitCode::IcebergError
        } else if error_str.contains("storage")
            || error_str.contains("s3")
            || error_str.contains("object_store")
        {
            ExitCode::StorageError
        } else if error_str.contains("txlog") || error_str.contains("transaction log") {
            ExitCode::TransactionLogError
        } else if error_str.contains("health") {
            ExitCode::HealthCheckError
        } else {
            ExitCode::RuntimeError
        }
    }
}

mod commands;
mod server;

#[derive(Parser)]
#[command(name = "k2i")]
#[command(about = "Kafka to Iceberg streaming ingestion CLI")]
#[command(
    long_about = "K2I is a single-process Kafka-to-Apache-Iceberg ingestion engine. It consumes Kafka messages, decodes configured payload formats, keeps recent rows in an Arrow hot buffer, writes Parquet files, commits Iceberg metadata, records durable progress in a transaction log, and exposes health, metrics, and optional local read-state RPC endpoints."
)]
#[command(after_long_help = "\
HTTP ENDPOINTS:
  /health   Full JSON health response
  /healthz  Liveness probe; stays 200 for degraded but operational states
  /readyz   Readiness probe; returns 503 for schema pauses and blockers
  /metrics  Prometheus metrics endpoint on the configured metrics port

LOCAL VERIFICATION:
  scripts/e2e-docker.sh
  K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-load.sh
  scripts/e2e-docker-iceberg.sh
  K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-iceberg-load.sh

The Iceberg load E2E validates real REST metadata and DuckDB iceberg_scan over
100,000 rows by default.")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Path to configuration file
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,

    /// Enable verbose logging (-v for debug, -vv for trace)
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the ingestion engine
    #[command(after_long_help = "\
EXAMPLES:
  k2i ingest --config config.toml
  k2i ingest --config /etc/k2i/production.toml
  k2i ingest --bootstrap-servers kafka1:9092,kafka2:9092 --topic events

BEHAVIOR:
  Starts Kafka consumption, health and metrics HTTP servers, optional read-state
  RPC, hot buffering, Parquet flushes, transaction-log writes, and Iceberg
  catalog commits. SIGINT and SIGTERM trigger graceful shutdown.")]
    Ingest {
        /// Override Kafka bootstrap servers
        #[arg(long)]
        bootstrap_servers: Option<String>,

        /// Override Kafka topic
        #[arg(long)]
        topic: Option<String>,

        /// Override consumer group
        #[arg(long)]
        consumer_group: Option<String>,
    },

    /// Show status and health
    #[command(after_long_help = "\
EXAMPLES:
  k2i status
  k2i status --url http://k2i-prod.internal:8080

BEHAVIOR:
  Queries /health at the provided URL, then derives /metrics by replacing port
  8080 with 9090.")]
    Status {
        /// Health endpoint URL
        #[arg(long, default_value = "http://localhost:8080")]
        url: String,
    },

    /// Run maintenance tasks manually
    #[command(after_long_help = "\
EXAMPLES:
  k2i maintenance compact --config config.toml
  k2i maintenance expire-snapshots --config config.toml
  k2i maintenance clean-orphans --config config.toml")]
    Maintenance {
        #[command(subcommand)]
        action: MaintenanceAction,
    },

    /// Register or reset table data
    #[command(after_long_help = "\
EXAMPLES:
  k2i table register --config config.toml --database f1 --table derived_state --source '/data/*.parquet'
  k2i table reset --config config.toml --database f1 --table derived_state --keep-data")]
    Table {
        #[command(subcommand)]
        action: TableAction,
    },

    /// Run a zero-cloud local development instance
    #[command(after_long_help = "\
EXAMPLES:
  k2i dev --topic events --warehouse ./warehouse
  k2i dev --topic f1.timing.derived_state --warehouse ./warehouse --schema-registry-url http://localhost:8081 --message-type f1.timing.v1.DerivedState

BEHAVIOR:
  Creates a local warehouse, transaction-log directory, runtime socket directory,
  SQLite catalog, filesystem object store, and enabled read-state RPC socket.")]
    Dev {
        /// Kafka topic to consume
        #[arg(long)]
        topic: String,

        /// Local warehouse directory
        #[arg(long)]
        warehouse: PathBuf,

        /// Kafka bootstrap servers
        #[arg(long, default_value = "localhost:9092")]
        bootstrap_servers: String,

        /// Database/namespace name
        #[arg(long, default_value = "default")]
        database: String,

        /// Table name
        #[arg(long, default_value = "events")]
        table: String,

        /// Consumer group
        #[arg(long, default_value = "k2i-dev")]
        consumer_group: String,

        /// Optional Confluent Schema Registry URL for Protobuf values
        #[arg(long)]
        schema_registry_url: Option<String>,

        /// Optional fully-qualified Protobuf message type
        #[arg(long)]
        message_type: Option<String>,
    },

    /// Validate configuration file
    #[command(after_long_help = "\
EXAMPLES:
  k2i validate --config config.toml
  k2i validate --config /etc/k2i/production.toml")]
    Validate,

    /// Generate shell completions and man pages
    #[command(after_long_help = "\
EXAMPLES:
  k2i completions bash > ~/.local/share/bash-completion/completions/k2i
  k2i completions zsh > ~/.zfunc/_k2i
  k2i completions fish > ~/.config/fish/completions/k2i.fish
  k2i completions power-shell > k2i.ps1
  k2i completions man --output-dir docs/man/man1")]
    Completions {
        #[command(subcommand)]
        action: CompletionAction,
    },
}

#[derive(Subcommand)]
enum CompletionAction {
    /// Generate bash completions
    #[command(
        after_long_help = "EXAMPLE:\n  k2i completions bash > ~/.local/share/bash-completion/completions/k2i"
    )]
    Bash,

    /// Generate zsh completions
    #[command(after_long_help = "EXAMPLE:\n  k2i completions zsh > ~/.zfunc/_k2i")]
    Zsh,

    /// Generate fish completions
    #[command(
        after_long_help = "EXAMPLE:\n  k2i completions fish > ~/.config/fish/completions/k2i.fish"
    )]
    Fish,

    /// Generate PowerShell completions
    #[command(after_long_help = "EXAMPLE:\n  k2i completions power-shell > k2i.ps1")]
    PowerShell,

    /// Generate recursive man page files
    #[command(after_long_help = "\
EXAMPLES:
  k2i completions man --output-dir docs/man/man1
  k2i completions man --output-dir /usr/local/share/man/man1
  man ./docs/man/man1/k2i.1")]
    Man {
        /// Directory to write .1 man page files into
        #[arg(long)]
        output_dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum TableAction {
    /// Register existing Parquet files without copying them
    #[command(after_long_help = "\
EXAMPLES:
  k2i table register --config config.toml --database f1 --table derived_state --source '/data/historical/*.parquet'
  k2i table register --config config.toml --database f1 --table derived_state --source /data/historical --schema-source schemas/derived_state.proto --message-type f1.timing.v1.DerivedState

BEHAVIOR:
  Expands files, directories, and globs. Infers schema from the first Parquet
  file unless a .proto schema source is supplied. Registers metadata through
  the configured catalog path and records transaction-log entries for recovery.
  Source files are not copied.")]
    Register {
        /// Database/namespace name
        #[arg(long)]
        database: String,

        /// Table name
        #[arg(long)]
        table: String,

        /// Source Parquet file, directory, or glob. Can be provided multiple times.
        #[arg(long = "source", required = true)]
        sources: Vec<String>,

        /// Schema source: first-file or path to a .proto file
        #[arg(long, default_value = "first-file")]
        schema_source: String,

        /// Fully-qualified Protobuf message type when --schema-source is a .proto file
        #[arg(long)]
        message_type: Option<String>,

        /// Optional topic label for read-state provenance
        #[arg(long)]
        kafka_topic: Option<String>,
    },

    /// Reset a registered table
    #[command(after_long_help = "\
EXAMPLE:
  k2i table reset --config config.toml --database f1 --table derived_state --keep-data")]
    Reset {
        /// Database/namespace name
        #[arg(long)]
        database: String,

        /// Table name
        #[arg(long)]
        table: String,

        /// Retain source data files
        #[arg(long)]
        keep_data: bool,
    },
}

#[derive(Subcommand)]
enum MaintenanceAction {
    /// Run compaction
    #[command(after_long_help = "\
BEHAVIOR:
  Scans the configured table for files smaller than compaction_threshold_mb,
  groups them toward compaction_target_mb, writes replacement Parquet files,
  and commits the resulting metadata through the configured catalog path.")]
    Compact,
    /// Expire old snapshots
    #[command(after_long_help = "\
BEHAVIOR:
  Identifies snapshots older than snapshot_retention_days and removes old
  snapshot metadata. Data files still referenced by retained snapshots are not
  deleted.")]
    ExpireSnapshots,
    /// Clean up orphan files
    #[command(after_long_help = "\
BEHAVIOR:
  Compares files present in the warehouse with files referenced by table
  snapshots and deletes orphan files older than orphan_retention_days.")]
    CleanOrphans,
}

#[tokio::main]
async fn main() {
    let exit_code = run_cli().await;
    std::process::exit(exit_code as i32);
}

/// Main CLI execution logic with proper error handling.
async fn run_cli() -> ExitCode {
    let cli = Cli::parse();

    // Try to load config for log format settings (optional - falls back to JSON)
    let log_format = cli
        .config
        .as_ref()
        .and_then(|path| std::fs::read_to_string(path).ok())
        .and_then(|content| toml::from_str::<Config>(&content).ok())
        .map(|config| config.monitoring.log_format)
        .unwrap_or(LogFormat::Json);

    // Initialize logging
    let filter = if std::env::var("RUST_LOG").is_ok() {
        EnvFilter::from_default_env()
    } else {
        match cli.verbose {
            0 => EnvFilter::new("info"),
            1 => EnvFilter::new("debug"),
            _ => EnvFilter::new("trace"),
        }
    };

    // Configure log format based on config
    match log_format {
        LogFormat::Json => {
            tracing_subscriber::registry()
                .with(fmt::layer().json())
                .with(filter)
                .init();
        }
        LogFormat::Text => {
            tracing_subscriber::registry()
                .with(fmt::layer())
                .with(filter)
                .init();
        }
    }

    let result = execute_command(cli).await;

    match result {
        Ok(()) => ExitCode::Success,
        Err(e) => {
            // Log the error
            tracing::error!(error = %e, "Command failed");

            // Determine appropriate exit code
            ExitCode::from_error(&e)
        }
    }
}

/// Execute the CLI command.
async fn execute_command(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Ingest {
            bootstrap_servers,
            topic,
            consumer_group,
        } => {
            let config = load_config(&cli.config)?;
            commands::ingest::run(config, bootstrap_servers, topic, consumer_group).await?;
        }

        Commands::Status { url } => {
            commands::status::run(&url).await?;
        }

        Commands::Maintenance { action } => {
            let config = load_config(&cli.config)?;
            match action {
                MaintenanceAction::Compact => {
                    commands::maintenance::compact(config).await?;
                }
                MaintenanceAction::ExpireSnapshots => {
                    commands::maintenance::expire_snapshots(config).await?;
                }
                MaintenanceAction::CleanOrphans => {
                    commands::maintenance::clean_orphans(config).await?;
                }
            }
        }

        Commands::Table { action } => {
            let config = load_config(&cli.config)?;
            match action {
                TableAction::Register {
                    database,
                    table,
                    sources,
                    schema_source,
                    message_type,
                    kafka_topic,
                } => {
                    commands::table::register(
                        config,
                        database,
                        table,
                        sources,
                        schema_source,
                        message_type,
                        kafka_topic,
                    )
                    .await?;
                }
                TableAction::Reset {
                    database,
                    table,
                    keep_data,
                } => {
                    commands::table::reset(config, database, table, keep_data).await?;
                }
            }
        }

        Commands::Dev {
            topic,
            warehouse,
            bootstrap_servers,
            database,
            table,
            consumer_group,
            schema_registry_url,
            message_type,
        } => {
            commands::dev::run(commands::dev::DevOptions {
                topic,
                warehouse,
                bootstrap_servers,
                database,
                table,
                consumer_group,
                schema_registry_url,
                message_type,
            })
            .await?;
        }

        Commands::Validate => {
            let config = load_config(&cli.config)?;
            config.validate()?;
            println!("Configuration is valid");
        }

        Commands::Completions { action } => {
            commands::completions::run(action)?;
        }
    }

    Ok(())
}

fn load_config(path: &Option<PathBuf>) -> Result<Config> {
    let path = path.clone().unwrap_or_else(|| PathBuf::from("config.toml"));

    let content = std::fs::read_to_string(&path)?;
    let config: Config = toml::from_str(&content)?;
    Ok(config)
}
