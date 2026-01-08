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
#[command(about = "Kafka to Iceberg streaming ingestion CLI", long_about = None)]
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
    Status {
        /// Health endpoint URL
        #[arg(long, default_value = "http://localhost:8080")]
        url: String,
    },

    /// Run maintenance tasks manually
    Maintenance {
        #[command(subcommand)]
        action: MaintenanceAction,
    },

    /// Validate configuration file
    Validate,
}

#[derive(Subcommand)]
enum MaintenanceAction {
    /// Run compaction
    Compact,
    /// Expire old snapshots
    ExpireSnapshots,
    /// Clean up orphan files
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

        Commands::Validate => {
            let config = load_config(&cli.config)?;
            config.validate()?;
            println!("Configuration is valid");
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
