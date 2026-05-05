//! Table management commands.

use anyhow::{bail, Context, Result};
use glob::glob;
use k2i_core::backfill::{
    register_parquet_files, reset_table_registration, BackfillRegisterOptions,
    BackfillResetOptions, BackfillSchemaSource,
};
use k2i_core::format::protobuf::ProtobufSchemaResolver;
use k2i_core::Config;
use std::path::PathBuf;

/// Register existing Parquet files as a K2I-readable table.
pub async fn register(
    config: Config,
    database: String,
    table: String,
    sources: Vec<String>,
    schema_source: String,
    message_type: Option<String>,
    kafka_topic: Option<String>,
) -> Result<()> {
    let source_files = expand_sources(&sources)?;
    let schema_source = parse_schema_source(&schema_source, message_type)?;

    let report = register_parquet_files(
        &config,
        BackfillRegisterOptions {
            database,
            table,
            source_files,
            schema_source,
            kafka_topic,
        },
    )
    .await?;

    println!(
        "registered {} files / {} rows for {}.{} at snapshot {}",
        report.files_registered,
        report.rows_registered,
        report.database,
        report.table,
        report.snapshot_id
    );
    Ok(())
}

/// Reset a table registration.
pub async fn reset(config: Config, database: String, table: String, keep_data: bool) -> Result<()> {
    reset_table_registration(
        &config,
        BackfillResetOptions {
            database: database.clone(),
            table: table.clone(),
            keep_data,
        },
    )
    .await?;

    println!("reset registration for {}.{}", database, table);
    Ok(())
}

fn parse_schema_source(
    schema_source: &str,
    message_type: Option<String>,
) -> Result<BackfillSchemaSource> {
    if schema_source == "first-file" {
        return Ok(BackfillSchemaSource::FirstFile);
    }

    let message_type = message_type
        .as_deref()
        .context("--message-type is required when --schema-source is a .proto file")?;
    let resolved = ProtobufSchemaResolver::resolve_proto_file(
        1,
        PathBuf::from(schema_source).as_path(),
        message_type,
    )
    .with_context(|| format!("failed to resolve protobuf schema {}", schema_source))?;
    Ok(BackfillSchemaSource::TableSchema(resolved.table_schema))
}

fn expand_sources(sources: &[String]) -> Result<Vec<PathBuf>> {
    if sources.is_empty() {
        bail!("at least one --source value is required");
    }

    let mut files = Vec::new();
    for source in sources {
        let path = PathBuf::from(source);
        if path.is_dir() {
            for entry in std::fs::read_dir(&path)
                .with_context(|| format!("failed to read source directory {}", path.display()))?
            {
                let entry = entry?;
                let path = entry.path();
                if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                    files.push(path);
                }
            }
            continue;
        }

        if source.contains('*') || source.contains('?') || source.contains('[') {
            for entry in glob(source).with_context(|| format!("invalid glob {}", source))? {
                files.push(entry.with_context(|| format!("failed to expand glob {}", source))?);
            }
            continue;
        }

        files.push(path);
    }

    files.sort();
    files.dedup();
    if files.is_empty() {
        bail!("no Parquet files matched --source");
    }
    Ok(files)
}
