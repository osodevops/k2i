//! Maintenance command implementations.

use anyhow::Result;
use k2i_core::maintenance::{CompactionTask, ExpirationTask, OrphanCleanupTask};
use k2i_core::Config;
use tracing::info;

/// Run compaction.
pub async fn compact(config: Config) -> Result<()> {
    info!(
        table = %config.iceberg.table_name,
        threshold_mb = %config.maintenance.compaction_threshold_mb,
        target_mb = %config.maintenance.compaction_target_mb,
        "Running compaction"
    );

    println!("Running compaction...");
    println!(
        "  Table: {}.{}",
        config.iceberg.database_name, config.iceberg.table_name
    );
    println!(
        "  Threshold: {} MB (files smaller than this)",
        config.maintenance.compaction_threshold_mb
    );
    println!(
        "  Target: {} MB (merged file size)",
        config.maintenance.compaction_target_mb
    );
    println!();

    let task = CompactionTask::new(
        config.maintenance.compaction_threshold_mb,
        config.maintenance.compaction_target_mb,
        None, // No txlog for CLI operation
    );

    let result = task.run().await?;

    println!("Compaction completed:");
    println!("  Files compacted: {}", result.files_compacted);
    println!("  Files created: {}", result.files_created);
    println!(
        "  Bytes saved: {} ({:.2} MB)",
        result.bytes_saved,
        result.bytes_saved as f64 / 1024.0 / 1024.0
    );

    if !result.processed_files.is_empty() {
        println!("\nProcessed files:");
        for file in &result.processed_files {
            println!("  - {}", file);
        }
    }

    Ok(())
}

/// Expire old snapshots.
pub async fn expire_snapshots(config: Config) -> Result<()> {
    info!(
        table = %config.iceberg.table_name,
        retention_days = %config.maintenance.snapshot_retention_days,
        "Expiring old snapshots"
    );

    println!("Expiring old snapshots...");
    println!(
        "  Table: {}.{}",
        config.iceberg.database_name, config.iceberg.table_name
    );
    println!(
        "  Retention: {} days",
        config.maintenance.snapshot_retention_days
    );
    println!();

    let task = ExpirationTask::new(
        config.maintenance.snapshot_retention_days,
        None, // No txlog for CLI operation
    );

    let result = task.run().await?;

    println!("Expiration completed:");
    println!("  Snapshots expired: {}", result.snapshots_expired);
    println!("  Manifests deleted: {}", result.manifests_deleted);

    if !result.expired_snapshot_ids.is_empty() {
        println!("\nExpired snapshot IDs:");
        for id in &result.expired_snapshot_ids {
            println!("  - {}", id);
        }
    }

    Ok(())
}

/// Clean up orphan files.
pub async fn clean_orphans(config: Config) -> Result<()> {
    info!(
        table = %config.iceberg.table_name,
        retention_days = %config.maintenance.orphan_retention_days,
        "Cleaning orphan files"
    );

    println!("Cleaning orphan files...");
    println!(
        "  Table: {}.{}",
        config.iceberg.database_name, config.iceberg.table_name
    );
    println!(
        "  Retention: {} days (safety buffer)",
        config.maintenance.orphan_retention_days
    );
    println!();

    let task = OrphanCleanupTask::new(
        config.maintenance.orphan_retention_days,
        None, // No txlog for CLI operation
    );

    let result = task.run().await?;

    println!("Orphan cleanup completed:");
    println!("  Files deleted: {}", result.files_deleted);
    println!(
        "  Bytes freed: {} ({:.2} MB)",
        result.bytes_freed,
        result.bytes_freed as f64 / 1024.0 / 1024.0
    );

    if !result.deleted_paths.is_empty() {
        println!("\nDeleted files:");
        for path in &result.deleted_paths {
            println!("  - {}", path);
        }
    }

    Ok(())
}
