//! SQL-backed catalog for local-first deployments.

use crate::config::{CatalogType, IcebergConfig, SqlCatalogBackend};
use crate::iceberg::factory::{
    CatalogFactory, CatalogHealth, CatalogOperations, DataFileInfo, SnapshotCommit,
    SnapshotCommitResult, TableInfo, TableSchema,
};
use crate::{Error, IcebergError, Result};
use async_trait::async_trait;
use sqlx::any::{install_default_drivers, AnyPoolOptions, AnyQueryResult, AnyRow};
use sqlx::migrate::MigrateDatabase;
use sqlx::{Any, AnyPool, Row, Transaction};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

const TABLES_SQL: &str = "k2i_catalog_tables";
const NAMESPACES_SQL: &str = "k2i_catalog_namespaces";
const DATA_FILES_SQL: &str = "k2i_catalog_data_files";

/// Factory for the SQL catalog.
pub struct SqlCatalogFactory;

#[async_trait]
impl CatalogFactory for SqlCatalogFactory {
    async fn create(&self, config: &IcebergConfig) -> Result<Arc<dyn CatalogOperations>> {
        Ok(Arc::new(SqlCatalogClient::connect(config).await?))
    }

    fn catalog_type(&self) -> CatalogType {
        CatalogType::Sql
    }
}

/// SQL catalog client used by K2I's catalog abstraction.
pub struct SqlCatalogClient {
    pool: AnyPool,
    catalog_name: String,
    backend: SqlCatalogBackend,
    warehouse_path: String,
}

impl SqlCatalogClient {
    /// Connect to the configured SQL catalog and create the catalog schema.
    pub async fn connect(config: &IcebergConfig) -> Result<Self> {
        let sql_config = config.sql_catalog.as_ref().ok_or_else(|| {
            Error::Config("iceberg.sql_catalog is required for SQL catalog".into())
        })?;

        install_default_drivers();
        let url = normalize_sql_url(sql_config.r#type.clone(), &sql_config.url)?;
        if sql_config.r#type == SqlCatalogBackend::Sqlite {
            if let Some(path) = sqlite_path(&url) {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
            }
            if !sqlx::Sqlite::database_exists(&url).await.unwrap_or(false) {
                sqlx::Sqlite::create_database(&url)
                    .await
                    .map_err(sql_error)?;
            }
        }
        let pool = AnyPoolOptions::new()
            .max_connections(config.catalog_manager.connection_pool_size as u32)
            .connect(&url)
            .await
            .map_err(|e| {
                Error::Iceberg(IcebergError::CatalogConnection(format!(
                    "failed to connect SQL catalog: {}",
                    e
                )))
            })?;

        let client = Self {
            pool,
            catalog_name: sql_config.catalog_name.clone(),
            backend: sql_config.r#type.clone(),
            warehouse_path: config.warehouse_path.clone(),
        };
        client.create_schema().await?;

        info!(
            catalog = %client.catalog_name,
            backend = ?client.backend,
            warehouse = %client.warehouse_path,
            "SQL catalog initialized"
        );

        Ok(client)
    }

    async fn create_schema(&self) -> Result<()> {
        self.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {NAMESPACES_SQL} (
                    catalog_name TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    properties_json TEXT NOT NULL,
                    created_at_ms TEXT NOT NULL,
                    PRIMARY KEY (catalog_name, namespace)
                )"
            ),
            vec![],
        )
        .await?;

        self.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {TABLES_SQL} (
                    catalog_name TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    location TEXT NOT NULL,
                    current_snapshot_id TEXT,
                    schema_json TEXT NOT NULL,
                    properties_json TEXT NOT NULL,
                    created_at_ms TEXT NOT NULL,
                    updated_at_ms TEXT NOT NULL,
                    PRIMARY KEY (catalog_name, namespace, table_name)
                )"
            ),
            vec![],
        )
        .await?;

        self.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {DATA_FILES_SQL} (
                    catalog_name TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    snapshot_id TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    file_size_bytes TEXT NOT NULL,
                    record_count TEXT NOT NULL,
                    partition_values_json TEXT NOT NULL,
                    file_format TEXT NOT NULL,
                    created_at_ms TEXT NOT NULL,
                    PRIMARY KEY (catalog_name, namespace, table_name, file_path)
                )"
            ),
            vec![],
        )
        .await?;

        Ok(())
    }

    fn replace_placeholders(&self, query: &str) -> String {
        match self.backend {
            SqlCatalogBackend::Postgres => {
                let mut count = 1;
                query
                    .chars()
                    .fold(String::with_capacity(query.len()), |mut acc, c| {
                        if c == '?' {
                            acc.push('$');
                            acc.push_str(&count.to_string());
                            count += 1;
                        } else {
                            acc.push(c);
                        }
                        acc
                    })
            }
            SqlCatalogBackend::Sqlite => query.to_string(),
        }
    }

    async fn fetch_all(&self, query: &str, args: Vec<Option<&str>>) -> Result<Vec<AnyRow>> {
        let query = self.replace_placeholders(query);
        let mut sqlx_query = sqlx::query(&query);
        for arg in args {
            sqlx_query = sqlx_query.bind(arg);
        }

        sqlx_query.fetch_all(&self.pool).await.map_err(sql_error)
    }

    async fn execute(&self, query: &str, args: Vec<Option<&str>>) -> Result<AnyQueryResult> {
        let query = self.replace_placeholders(query);
        let mut sqlx_query = sqlx::query(&query);
        for arg in args {
            sqlx_query = sqlx_query.bind(arg);
        }

        sqlx_query.execute(&self.pool).await.map_err(sql_error)
    }

    async fn execute_tx(
        &self,
        tx: &mut Transaction<'_, Any>,
        query: &str,
        args: Vec<Option<&str>>,
    ) -> Result<AnyQueryResult> {
        let query = self.replace_placeholders(query);
        let mut sqlx_query = sqlx::query(&query);
        for arg in args {
            sqlx_query = sqlx_query.bind(arg);
        }

        sqlx_query.execute(&mut **tx).await.map_err(sql_error)
    }

    fn table_location(&self, namespace: &str, table: &str) -> String {
        format!(
            "{}/{}/{}",
            self.warehouse_path.trim_end_matches('/'),
            namespace,
            table
        )
    }

    fn row_to_table(&self, row: &AnyRow) -> Result<TableInfo> {
        let schema_json: String = row.try_get("schema_json").map_err(sql_error)?;
        let properties_json: String = row.try_get("properties_json").map_err(sql_error)?;
        let snapshot: Option<String> = row.try_get("current_snapshot_id").map_err(sql_error)?;

        Ok(TableInfo {
            namespace: row.try_get("namespace").map_err(sql_error)?,
            name: row.try_get("table_name").map_err(sql_error)?,
            location: row.try_get("location").map_err(sql_error)?,
            current_snapshot_id: snapshot
                .as_deref()
                .map(str::parse::<i64>)
                .transpose()
                .map_err(|e| {
                    Error::Iceberg(IcebergError::CatalogConnection(format!(
                        "invalid snapshot ID in SQL catalog: {}",
                        e
                    )))
                })?,
            schema: serde_json::from_str(&schema_json)?,
            properties: serde_json::from_str(&properties_json)?,
        })
    }

    /// Return the files tracked for a table.
    pub async fn list_data_files(&self, namespace: &str, table: &str) -> Result<Vec<DataFileInfo>> {
        let rows = self
            .fetch_all(
                &format!(
                    "SELECT file_path, file_size_bytes, record_count, partition_values_json, file_format
                     FROM {DATA_FILES_SQL}
                     WHERE catalog_name = ? AND namespace = ? AND table_name = ?
                     ORDER BY created_at_ms, file_path"
                ),
                vec![Some(&self.catalog_name), Some(namespace), Some(table)],
            )
            .await?;

        rows.into_iter()
            .map(|row| {
                let partition_values_json: String =
                    row.try_get("partition_values_json").map_err(sql_error)?;
                let file_size_bytes: String = row.try_get("file_size_bytes").map_err(sql_error)?;
                let record_count: String = row.try_get("record_count").map_err(sql_error)?;
                Ok(DataFileInfo {
                    file_path: row.try_get("file_path").map_err(sql_error)?,
                    file_size_bytes: file_size_bytes.parse().map_err(|e| {
                        Error::Iceberg(IcebergError::CatalogConnection(format!(
                            "invalid file size in SQL catalog: {}",
                            e
                        )))
                    })?,
                    record_count: record_count.parse().map_err(|e| {
                        Error::Iceberg(IcebergError::CatalogConnection(format!(
                            "invalid record count in SQL catalog: {}",
                            e
                        )))
                    })?,
                    partition_values: serde_json::from_str(&partition_values_json)?,
                    file_format: row.try_get("file_format").map_err(sql_error)?,
                })
            })
            .collect()
    }

    /// Remove a registered table from the SQL catalog.
    pub async fn reset_table(&self, namespace: &str, table: &str) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(sql_error)?;
        self.execute_tx(
            &mut tx,
            &format!(
                "DELETE FROM {DATA_FILES_SQL}
                 WHERE catalog_name = ? AND namespace = ? AND table_name = ?"
            ),
            vec![Some(&self.catalog_name), Some(namespace), Some(table)],
        )
        .await?;
        self.execute_tx(
            &mut tx,
            &format!(
                "DELETE FROM {TABLES_SQL}
                 WHERE catalog_name = ? AND namespace = ? AND table_name = ?"
            ),
            vec![Some(&self.catalog_name), Some(namespace), Some(table)],
        )
        .await?;
        tx.commit().await.map_err(sql_error)?;
        Ok(())
    }
}

#[async_trait]
impl CatalogOperations for SqlCatalogClient {
    async fn health_check(&self) -> Result<CatalogHealth> {
        let start = Instant::now();
        self.execute("SELECT 1", vec![]).await?;
        Ok(CatalogHealth {
            is_healthy: true,
            response_time_ms: start.elapsed().as_millis() as u64,
            message: None,
            catalog_type: CatalogType::Sql,
        })
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let rows = self
            .fetch_all(
                &format!(
                    "SELECT namespace FROM {NAMESPACES_SQL}
                     WHERE catalog_name = ?
                     ORDER BY namespace"
                ),
                vec![Some(&self.catalog_name)],
            )
            .await?;
        rows.into_iter()
            .map(|row| row.try_get("namespace").map_err(sql_error))
            .collect()
    }

    async fn namespace_exists(&self, namespace: &str) -> Result<bool> {
        let rows = self
            .fetch_all(
                &format!(
                    "SELECT namespace FROM {NAMESPACES_SQL}
                     WHERE catalog_name = ? AND namespace = ?"
                ),
                vec![Some(&self.catalog_name), Some(namespace)],
            )
            .await?;
        Ok(!rows.is_empty())
    }

    async fn create_namespace(&self, namespace: &str) -> Result<()> {
        if self.namespace_exists(namespace).await? {
            return Ok(());
        }

        let now = chrono::Utc::now().timestamp_millis().to_string();
        self.execute(
            &format!(
                "INSERT INTO {NAMESPACES_SQL}
                 (catalog_name, namespace, properties_json, created_at_ms)
                 VALUES (?, ?, ?, ?)"
            ),
            vec![
                Some(&self.catalog_name),
                Some(namespace),
                Some("{}"),
                Some(&now),
            ],
        )
        .await?;
        Ok(())
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let rows = self
            .fetch_all(
                &format!(
                    "SELECT table_name FROM {TABLES_SQL}
                     WHERE catalog_name = ? AND namespace = ?
                     ORDER BY table_name"
                ),
                vec![Some(&self.catalog_name), Some(namespace)],
            )
            .await?;
        rows.into_iter()
            .map(|row| row.try_get("table_name").map_err(sql_error))
            .collect()
    }

    async fn table_exists(&self, namespace: &str, table: &str) -> Result<bool> {
        let rows = self
            .fetch_all(
                &format!(
                    "SELECT table_name FROM {TABLES_SQL}
                     WHERE catalog_name = ? AND namespace = ? AND table_name = ?"
                ),
                vec![Some(&self.catalog_name), Some(namespace), Some(table)],
            )
            .await?;
        Ok(!rows.is_empty())
    }

    async fn load_table(&self, namespace: &str, table: &str) -> Result<TableInfo> {
        let rows = self
            .fetch_all(
                &format!(
                    "SELECT namespace, table_name, location, current_snapshot_id, schema_json, properties_json
                     FROM {TABLES_SQL}
                     WHERE catalog_name = ? AND namespace = ? AND table_name = ?"
                ),
                vec![Some(&self.catalog_name), Some(namespace), Some(table)],
            )
            .await?;

        let row = rows.first().ok_or_else(|| {
            Error::Iceberg(IcebergError::TableNotFound(format!(
                "{}.{}",
                namespace, table
            )))
        })?;
        self.row_to_table(row)
    }

    async fn create_table(
        &self,
        namespace: &str,
        table: &str,
        schema: &TableSchema,
    ) -> Result<TableInfo> {
        self.create_namespace(namespace).await?;
        if self.table_exists(namespace, table).await? {
            return self.load_table(namespace, table).await;
        }

        let now = chrono::Utc::now().timestamp_millis().to_string();
        let schema_json = serde_json::to_string(schema)?;
        let properties = HashMap::<String, String>::new();
        let properties_json = serde_json::to_string(&properties)?;
        let location = self.table_location(namespace, table);

        self.execute(
            &format!(
                "INSERT INTO {TABLES_SQL}
                 (catalog_name, namespace, table_name, location, current_snapshot_id, schema_json, properties_json, created_at_ms, updated_at_ms)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            ),
            vec![
                Some(&self.catalog_name),
                Some(namespace),
                Some(table),
                Some(&location),
                None,
                Some(&schema_json),
                Some(&properties_json),
                Some(&now),
                Some(&now),
            ],
        )
        .await?;

        Ok(TableInfo {
            namespace: namespace.to_string(),
            name: table.to_string(),
            location,
            current_snapshot_id: None,
            schema: schema.clone(),
            properties,
        })
    }

    async fn current_snapshot_id(&self, namespace: &str, table: &str) -> Result<Option<i64>> {
        Ok(self.load_table(namespace, table).await?.current_snapshot_id)
    }

    async fn commit_snapshot(
        &self,
        namespace: &str,
        table: &str,
        commit: SnapshotCommit,
    ) -> Result<SnapshotCommitResult> {
        let current = self.current_snapshot_id(namespace, table).await?;
        if current != commit.expected_snapshot_id {
            return Err(Error::Iceberg(IcebergError::CasConflict {
                expected: commit.expected_snapshot_id.unwrap_or(-1),
                actual: current.unwrap_or(-1),
            }));
        }

        let snapshot_id = chrono::Utc::now().timestamp_micros();
        let snapshot_id_string = snapshot_id.to_string();
        let now = chrono::Utc::now();
        let now_ms = now.timestamp_millis().to_string();
        let mut tx = self.pool.begin().await.map_err(sql_error)?;

        for file in &commit.files_to_add {
            let file_size = file.file_size_bytes.to_string();
            let record_count = file.record_count.to_string();
            let partition_values_json = serde_json::to_string(&file.partition_values)?;
            self.execute_tx(
                &mut tx,
                &format!(
                    "INSERT INTO {DATA_FILES_SQL}
                     (catalog_name, namespace, table_name, snapshot_id, file_path, file_size_bytes, record_count, partition_values_json, file_format, created_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                ),
                vec![
                    Some(&self.catalog_name),
                    Some(namespace),
                    Some(table),
                    Some(&snapshot_id_string),
                    Some(&file.file_path),
                    Some(&file_size),
                    Some(&record_count),
                    Some(&partition_values_json),
                    Some(&file.file_format),
                    Some(&now_ms),
                ],
            )
            .await?;
        }

        let updated = self
            .execute_tx(
                &mut tx,
                &format!(
                    "UPDATE {TABLES_SQL}
                     SET current_snapshot_id = ?, updated_at_ms = ?
                     WHERE catalog_name = ? AND namespace = ? AND table_name = ?"
                ),
                vec![
                    Some(&snapshot_id_string),
                    Some(&now_ms),
                    Some(&self.catalog_name),
                    Some(namespace),
                    Some(table),
                ],
            )
            .await?;
        if updated.rows_affected() == 0 {
            return Err(Error::Iceberg(IcebergError::TableNotFound(format!(
                "{}.{}",
                namespace, table
            ))));
        }

        tx.commit().await.map_err(sql_error)?;

        debug!(
            namespace = %namespace,
            table = %table,
            snapshot_id = snapshot_id,
            files = commit.files_to_add.len(),
            "Committed SQL catalog snapshot"
        );

        Ok(SnapshotCommitResult {
            snapshot_id,
            committed_at: now,
            files_added: commit.files_to_add.len(),
            files_removed: commit.files_to_remove.len(),
        })
    }

    async fn update_schema(
        &self,
        namespace: &str,
        table: &str,
        schema: &TableSchema,
        expected_schema_id: Option<i32>,
    ) -> Result<TableInfo> {
        let existing = self.load_table(namespace, table).await?;
        if expected_schema_id.is_some() && expected_schema_id != Some(existing.schema.schema_id) {
            return Err(Error::Iceberg(IcebergError::SchemaEvolution(format!(
                "schema update conflict for {}.{}: expected {:?}, actual {}",
                namespace, table, expected_schema_id, existing.schema.schema_id
            ))));
        }

        let schema_json = serde_json::to_string(schema)?;
        let now_ms = chrono::Utc::now().timestamp_millis().to_string();
        self.execute(
            &format!(
                "UPDATE {TABLES_SQL}
                 SET schema_json = ?, updated_at_ms = ?
                 WHERE catalog_name = ? AND namespace = ? AND table_name = ?"
            ),
            vec![
                Some(&schema_json),
                Some(&now_ms),
                Some(&self.catalog_name),
                Some(namespace),
                Some(table),
            ],
        )
        .await?;

        self.load_table(namespace, table).await
    }

    fn catalog_type(&self) -> CatalogType {
        CatalogType::Sql
    }

    fn warehouse_path(&self) -> &str {
        &self.warehouse_path
    }

    async fn close(&self) -> Result<()> {
        self.pool.close().await;
        Ok(())
    }
}

fn sql_error(error: sqlx::Error) -> Error {
    Error::Iceberg(IcebergError::CatalogConnection(error.to_string()))
}

fn normalize_sql_url(backend: SqlCatalogBackend, url: &str) -> Result<String> {
    if backend != SqlCatalogBackend::Sqlite {
        return Ok(url.to_string());
    }

    if url == "sqlite::memory:" || url == "sqlite::memory:?cache=shared" {
        return Ok(url.to_string());
    }

    let path = if let Some(path) = url.strip_prefix("sqlite://") {
        path
    } else if let Some(path) = url.strip_prefix("sqlite:") {
        path
    } else {
        return Ok(url.to_string());
    };

    if path.is_empty() {
        return Err(Error::Config(
            "sqlite catalog URL must include a path".into(),
        ));
    }

    Ok(format!("sqlite:{}", path))
}

fn sqlite_path(url: &str) -> Option<std::path::PathBuf> {
    if url == "sqlite::memory:" || url == "sqlite::memory:?cache=shared" {
        return None;
    }
    url.strip_prefix("sqlite:").map(std::path::PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CatalogManagerConfig, ObjectStoreConfig, ParquetCompression, SqlCatalogConfig,
    };
    use tempfile::TempDir;

    fn config(temp: &TempDir) -> IcebergConfig {
        IcebergConfig {
            catalog_type: CatalogType::Sql,
            warehouse_path: temp.path().join("warehouse").to_string_lossy().to_string(),
            database_name: "f1".to_string(),
            table_name: "historical".to_string(),
            target_file_size_mb: 512,
            compression: ParquetCompression::Snappy,
            partition_spec: vec![],
            rest_uri: None,
            hive_metastore_uri: None,
            aws_region: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            s3_endpoint: None,
            catalog_manager: CatalogManagerConfig::default(),
            table_management: Default::default(),
            rest: Default::default(),
            glue: Default::default(),
            nessie: None,
            sql_catalog: Some(SqlCatalogConfig {
                r#type: SqlCatalogBackend::Sqlite,
                url: format!("sqlite://{}", temp.path().join("catalog.db").display()),
                catalog_name: "k2i_test".to_string(),
            }),
            object_store: ObjectStoreConfig::default(),
        }
    }

    fn schema() -> TableSchema {
        TableSchema {
            schema_id: 1,
            fields: vec![crate::iceberg::factory::SchemaFieldInfo {
                id: 1,
                name: "driver".to_string(),
                field_type: "string".to_string(),
                required: true,
                doc: None,
            }],
        }
    }

    #[tokio::test]
    async fn sql_catalog_create_commit_and_reload() {
        let temp = TempDir::new().unwrap();
        let config = config(&temp);
        let catalog = SqlCatalogClient::connect(&config).await.unwrap();

        catalog
            .create_table("f1", "historical", &schema())
            .await
            .unwrap();
        assert!(catalog.table_exists("f1", "historical").await.unwrap());

        let result = catalog
            .commit_snapshot(
                "f1",
                "historical",
                SnapshotCommit {
                    expected_snapshot_id: None,
                    files_to_add: vec![DataFileInfo {
                        file_path: "/tmp/historical.parquet".to_string(),
                        file_size_bytes: 42,
                        record_count: 2,
                        partition_values: HashMap::new(),
                        file_format: "parquet".to_string(),
                    }],
                    files_to_remove: vec![],
                    summary: HashMap::new(),
                },
            )
            .await
            .unwrap();

        let table = catalog.load_table("f1", "historical").await.unwrap();
        assert_eq!(table.current_snapshot_id, Some(result.snapshot_id));
        let files = catalog.list_data_files("f1", "historical").await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].record_count, 2);
    }
}
