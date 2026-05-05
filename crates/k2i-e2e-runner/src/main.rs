use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, StringArray};
use arrow::ipc::reader::StreamReader;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::{SinkExt, StreamExt};
use k2i_rpc::{decode_response, encode_request, ReadState, RpcRequest, RpcResponse};
use prost::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};
use tokio::net::UnixStream;
use tokio::time::sleep;
use tokio_util::codec::LengthDelimitedCodec;

const SUBJECT: &str = "f1.timing.derived_state-value";
const DATABASE: &str = "f1";
const TABLE: &str = "derived_state";

#[derive(Debug, Clone)]
struct E2eConfig {
    kafka_bootstrap: String,
    schema_registry_url: String,
    health_url: String,
    metrics_url: String,
    rpc_socket: PathBuf,
    warehouse_path: PathBuf,
    topic: String,
    max_frame_bytes: usize,
}

impl E2eConfig {
    fn from_env() -> Self {
        Self {
            kafka_bootstrap: env::var("K2I_E2E_KAFKA_BOOTSTRAP")
                .unwrap_or_else(|_| "kafka:29092".to_string()),
            schema_registry_url: env::var("K2I_E2E_SCHEMA_REGISTRY")
                .unwrap_or_else(|_| "http://schema-registry:8081".to_string()),
            health_url: env::var("K2I_E2E_HEALTH_URL")
                .unwrap_or_else(|_| "http://k2i:8080".to_string()),
            metrics_url: env::var("K2I_E2E_METRICS_URL")
                .unwrap_or_else(|_| "http://k2i:9090".to_string()),
            rpc_socket: env::var("K2I_E2E_RPC_SOCKET")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("/var/run/k2i/k2i.sock")),
            warehouse_path: env::var("K2I_E2E_WAREHOUSE")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("/var/lib/k2i/warehouse")),
            topic: env::var("K2I_E2E_TOPIC")
                .unwrap_or_else(|_| "f1.timing.derived_state".to_string()),
            max_frame_bytes: env::var("K2I_E2E_MAX_FRAME_BYTES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(k2i_rpc::DEFAULT_MAX_FRAME_BYTES),
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct Telemetry {
    #[prost(double, tag = "1")]
    speed: f64,
    #[prost(int32, tag = "2")]
    gear: i32,
}

#[derive(Clone, PartialEq, Message)]
struct DerivedStateV1 {
    #[prost(string, tag = "1")]
    driver: String,
    #[prost(int32, tag = "2")]
    lap: i32,
    #[prost(double, tag = "3")]
    sector_time: f64,
    #[prost(message, optional, tag = "4")]
    telemetry: Option<Telemetry>,
    #[prost(double, repeated, tag = "5")]
    samples: Vec<f64>,
    #[prost(map = "string, int32", tag = "6")]
    counters: HashMap<String, i32>,
    #[prost(string, tag = "7")]
    status_text: String,
}

#[derive(Clone, PartialEq, Message)]
struct DerivedStateV2 {
    #[prost(string, tag = "1")]
    driver: String,
    #[prost(int32, tag = "2")]
    lap: i32,
    #[prost(double, tag = "3")]
    sector_time: f64,
    #[prost(message, optional, tag = "4")]
    telemetry: Option<Telemetry>,
    #[prost(double, repeated, tag = "5")]
    samples: Vec<f64>,
    #[prost(map = "string, int32", tag = "6")]
    counters: HashMap<String, i32>,
    #[prost(string, tag = "7")]
    status_text: String,
    #[prost(string, tag = "9")]
    team: String,
}

#[derive(Clone, PartialEq, Message)]
struct DerivedStateV3Breaking {
    #[prost(string, tag = "1")]
    driver: String,
    #[prost(string, tag = "2")]
    lap: String,
    #[prost(double, tag = "3")]
    sector_time: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = E2eConfig::from_env();

    if env::var("K2I_E2E_MODE").as_deref() == Ok("backfill-init") {
        run_backfill_init().await?;
        return Ok(());
    }
    if env::var("K2I_E2E_MODE").as_deref() == Ok("backfill-verify") {
        run_backfill_verify(&config).await?;
        return Ok(());
    }

    wait_for_http(&format!("{}/subjects", config.schema_registry_url)).await?;
    if env::var("K2I_E2E_MODE").as_deref() == Ok("schema-init") {
        set_subject_compatibility_none(&config).await?;
        let schema_id = register_schema(&config, schema_v1()).await?;
        println!("ok: registered startup Protobuf schema ID {}", schema_id);
        return Ok(());
    }
    if env::var("K2I_E2E_MODE").as_deref() == Ok("load") {
        run_load(&config).await?;
        return Ok(());
    }
    if env::var("K2I_E2E_MODE").as_deref() == Ok("iceberg") {
        run_iceberg(&config).await?;
        return Ok(());
    }
    if env::var("K2I_E2E_MODE").as_deref() == Ok("iceberg-load") {
        run_iceberg_load(&config).await?;
        return Ok(());
    }

    create_topic(&config).await?;
    wait_for_rpc(&config).await?;
    set_subject_compatibility_none(&config).await?;

    let v1_id = register_schema(&config, schema_v1()).await?;
    produce_v1(&config, v1_id, 1..=3).await?;
    let hot_v1 = wait_for_state(
        "three v1 hot rows",
        &config,
        Duration::from_secs(45),
        |state| Ok(visible_row_count(state)? == 3 && state.data_files.is_empty()),
    )
    .await?;
    assert_hot_rows(&hot_v1, 3, None)?;
    println!("ok: v1 hot rows visible through RPC");

    produce_v1(&config, v1_id, 4..=5).await?;
    let cold_v1 = wait_for_state(
        "first flush to cold parquet",
        &config,
        Duration::from_secs(60),
        |state| Ok(cold_row_count(state) >= 5),
    )
    .await?;
    assert_eq!(
        duckdb_count(&cold_v1)?,
        5,
        "DuckDB should count v1 cold parquet rows"
    );
    println!("ok: v1 cold parquet rows visible to DuckDB");

    let v2_id = register_schema(&config, schema_v2()).await?;
    produce_v2(&config, v2_id, 6..=7).await?;
    let hot_v2 = wait_for_state(
        "additive v2 hot rows",
        &config,
        Duration::from_secs(60),
        |state| Ok(visible_row_count(state)? == 7 && hot_row_count(state)? == 2),
    )
    .await?;
    assert_hot_rows(&hot_v2, 2, Some("team"))?;
    println!("ok: additive v2 schema appears in hot Arrow IPC");

    produce_v2(&config, v2_id, 8..=10).await?;
    let cold_v2 = wait_for_state(
        "second flush with evolved schema",
        &config,
        Duration::from_secs(60),
        |state| Ok(cold_row_count(state) >= 10),
    )
    .await?;
    assert_eq!(
        duckdb_count(&cold_v2)?,
        10,
        "DuckDB should union v1/v2 parquet schemas by name"
    );
    assert_duckdb_team_values(&cold_v2)?;
    println!("ok: evolved cold parquet rows visible to DuckDB");

    let v3_id = register_schema(&config, schema_v3_breaking()).await?;
    produce_v3_breaking(&config, v3_id, 11).await?;
    let after_breaking = wait_for_state(
        "breaking row rejected from visible state",
        &config,
        Duration::from_secs(20),
        |state| Ok(visible_row_count(state)? == 10),
    )
    .await?;
    assert_eq!(visible_row_count(&after_breaking)?, 10);
    assert_schema_degraded(&config).await?;
    assert_readyz_blocked(&config).await?;
    produce_v2(&config, v2_id, 12..=12).await?;
    sleep(Duration::from_secs(2)).await;
    let after_pause = scan_begin(&config).await?;
    assert_eq!(
        visible_row_count(&after_pause)?,
        10,
        "valid data produced after a schema pause must not be ingested"
    );
    println!("ok: breaking schema did not become visible");

    Ok(())
}

async fn run_backfill_init() -> Result<()> {
    let historical_dir = env::var("K2I_E2E_HISTORICAL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/historical"));
    let config_path = env::var("K2I_E2E_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/etc/k2i/config.toml"));
    let database = env::var("K2I_E2E_DATABASE").unwrap_or_else(|_| DATABASE.to_string());
    let table = env::var("K2I_E2E_TABLE").unwrap_or_else(|_| TABLE.to_string());

    std::fs::create_dir_all(&historical_dir)
        .with_context(|| format!("failed to create {}", historical_dir.display()))?;
    let parquet_path = historical_dir.join("historical.parquet");
    let script = r#"
import duckdb
import os
path = os.environ["K2I_E2E_PARQUET_PATH"]
duckdb.connect().execute("""
    COPY (
      SELECT
        'DRV' || i::VARCHAR AS driver,
        i::INTEGER AS lap,
        (28.0 + i / 100.0)::DOUBLE AS sector_time
      FROM range(1, 6) AS t(i)
    )
    TO ? (FORMAT PARQUET)
""", [path])
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(script)
        .env("K2I_E2E_PARQUET_PATH", &parquet_path)
        .output()
        .context("failed to create backfill Parquet fixture")?;
    if !output.status.success() {
        bail!(
            "backfill Parquet fixture creation failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let status = Command::new("cargo")
        .args([
            "run",
            "-p",
            "k2i-cli",
            "--",
            "--config",
            config_path
                .to_str()
                .ok_or_else(|| anyhow!("invalid config path {}", config_path.display()))?,
            "table",
            "register",
            "--database",
            &database,
            "--table",
            &table,
            "--source",
            parquet_path
                .to_str()
                .ok_or_else(|| anyhow!("invalid Parquet path {}", parquet_path.display()))?,
        ])
        .status()
        .context("failed to run k2i table register")?;
    if !status.success() {
        bail!("k2i table register failed with status {}", status);
    }

    println!("ok: registered backfill fixture {}", parquet_path.display());
    Ok(())
}

async fn run_backfill_verify(config: &E2eConfig) -> Result<()> {
    wait_for_rpc(config).await?;
    let state = wait_for_state(
        "backfilled rows visible",
        config,
        Duration::from_secs(90),
        |state| Ok(cold_row_count(state) == 5 && state.data_files.len() == 1),
    )
    .await?;
    assert_eq!(visible_row_count(&state)?, 5);
    assert_eq!(duckdb_count(&state)?, 5);
    println!("ok: backfilled Parquet visible through RPC and DuckDB");
    Ok(())
}

async fn run_load(config: &E2eConfig) -> Result<()> {
    create_topic(config).await?;
    wait_for_rpc(config).await?;
    set_subject_compatibility_none(config).await?;

    let (total, concurrency) = load_settings()?;

    let v1_id = register_schema(config, schema_v1()).await?;
    let v2_id = register_schema(config, schema_v2()).await?;
    let split = total / 2;

    let produce_start = Instant::now();
    produce_load_range(config, v1_id, 1..=split, None, concurrency).await?;
    produce_load_range(
        config,
        v2_id,
        split + 1..=total,
        Some("Red Bull"),
        concurrency,
    )
    .await?;
    let produce_elapsed = produce_start.elapsed();

    let ingest_start = Instant::now();
    let wait_timeout = load_timeout(Duration::from_secs(300))?;
    let state = wait_for_state("load rows visible", config, wait_timeout, |state| {
        Ok(visible_row_count(state)? >= total as u64)
    })
    .await?;
    let ingest_elapsed = ingest_start.elapsed();
    assert_eq!(visible_row_count(&state)?, total as u64);

    if cold_row_count(&state) == total as u64 {
        assert_eq!(duckdb_count(&state)?, total as u64);
    }
    assert_metrics_errors_zero(config).await?;

    let produced_per_sec = total as f64 / produce_elapsed.as_secs_f64().max(0.001);
    println!(
        "ok: load test produced and ingested {} rows; producer {:.0} rows/s; visibility wait {}ms",
        total,
        produced_per_sec,
        ingest_elapsed.as_millis()
    );
    Ok(())
}

async fn run_iceberg(config: &E2eConfig) -> Result<()> {
    create_topic(config).await?;
    wait_for_rpc(config).await?;
    set_subject_compatibility_none(config).await?;

    let schema_id = register_schema(config, schema_v1()).await?;
    produce_v1(config, schema_id, 1..=5).await?;
    let state = wait_for_state(
        "iceberg append rows visible",
        config,
        Duration::from_secs(90),
        |state| Ok(cold_row_count(state) >= 5),
    )
    .await?;
    assert_eq!(
        duckdb_count(&state)?,
        5,
        "DuckDB should count cold Parquet rows before Iceberg validation"
    );

    let iceberg_count = duckdb_iceberg_count(&config.warehouse_path)?;
    if iceberg_count != 5 {
        bail!(
            "expected DuckDB iceberg_scan to see 5 rows, got {}",
            iceberg_count
        );
    }
    assert_iceberg_metadata_files(&config.warehouse_path)?;

    println!("ok: real Iceberg metadata visible to DuckDB iceberg_scan");
    Ok(())
}

async fn run_iceberg_load(config: &E2eConfig) -> Result<()> {
    create_topic(config).await?;
    wait_for_rpc(config).await?;
    set_subject_compatibility_none(config).await?;

    let (total, concurrency) = load_settings()?;
    let wait_timeout = load_timeout(Duration::from_secs(600))?;
    let min_data_files = env_usize_or(
        "K2I_E2E_ICEBERG_LOAD_MIN_DATA_FILES",
        if total >= 10_000 { 2 } else { 1 },
    )?;
    let min_snapshots = env_usize_or(
        "K2I_E2E_ICEBERG_LOAD_MIN_SNAPSHOTS",
        if total >= 10_000 { 2 } else { 1 },
    )?;

    let schema_id = register_schema(config, schema_v1()).await?;

    let produce_start = Instant::now();
    produce_load_range(config, schema_id, 1..=total, None, concurrency).await?;
    let produce_elapsed = produce_start.elapsed();

    let ingest_start = Instant::now();
    let state = wait_for_state("iceberg load rows flushed", config, wait_timeout, |state| {
        Ok(cold_row_count(state) >= total as u64)
    })
    .await?;
    let ingest_elapsed = ingest_start.elapsed();

    let visible_rows = visible_row_count(&state)?;
    let cold_rows = cold_row_count(&state);
    if visible_rows != total as u64 {
        bail!(
            "expected {} visible rows after Iceberg load, got {}",
            total,
            visible_rows
        );
    }
    if cold_rows != total as u64 {
        bail!(
            "expected {} cold rows after Iceberg load, got {}",
            total,
            cold_rows
        );
    }
    if state.data_files.len() < min_data_files {
        bail!(
            "expected at least {} Iceberg data files, got {}",
            min_data_files,
            state.data_files.len()
        );
    }

    let parquet_count = duckdb_count(&state)?;
    if parquet_count != total as u64 {
        bail!(
            "expected DuckDB Parquet scan to see {} rows, got {}",
            total,
            parquet_count
        );
    }

    let iceberg_count = duckdb_iceberg_count(&config.warehouse_path)?;
    if iceberg_count != total as u64 {
        bail!(
            "expected DuckDB iceberg_scan to see {} rows, got {}",
            total,
            iceberg_count
        );
    }
    assert_iceberg_metadata_files(&config.warehouse_path)?;
    let snapshot_count = iceberg_snapshot_count(&config.warehouse_path)?;
    if snapshot_count < min_snapshots {
        bail!(
            "expected at least {} Iceberg snapshots, got {}",
            min_snapshots,
            snapshot_count
        );
    }
    assert_metrics_errors_zero(config).await?;

    let produced_per_sec = total as f64 / produce_elapsed.as_secs_f64().max(0.001);
    println!(
        "ok: iceberg load validated {} rows, {} data files, {} snapshots; producer {:.0} rows/s; cold visibility wait {}ms",
        total,
        state.data_files.len(),
        snapshot_count,
        produced_per_sec,
        ingest_elapsed.as_millis()
    );
    Ok(())
}

fn load_settings() -> Result<(i32, usize)> {
    let total = env_i32_or("K2I_E2E_LOAD_MESSAGES", 100_000)?;
    let concurrency = env_usize_or("K2I_E2E_LOAD_CONCURRENCY", 2_000)?;
    if total <= 0 {
        bail!("K2I_E2E_LOAD_MESSAGES must be greater than zero");
    }
    if concurrency == 0 {
        bail!("K2I_E2E_LOAD_CONCURRENCY must be greater than zero");
    }
    Ok((total, concurrency))
}

fn load_timeout(default: Duration) -> Result<Duration> {
    let seconds = env_u64_or("K2I_E2E_LOAD_TIMEOUT_SECONDS", default.as_secs())?;
    if seconds == 0 {
        bail!("K2I_E2E_LOAD_TIMEOUT_SECONDS must be greater than zero");
    }
    Ok(Duration::from_secs(seconds))
}

fn env_i32_or(name: &str, default: i32) -> Result<i32> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{} must be an integer", name)),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", name)),
    }
}

fn env_u64_or(name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{} must be an integer", name)),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", name)),
    }
}

fn env_usize_or(name: &str, default: usize) -> Result<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{} must be an integer", name)),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", name)),
    }
}

async fn wait_for_http(url: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        match client.get(url).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            _ if Instant::now() < deadline => sleep(Duration::from_millis(500)).await,
            _ => bail!("timed out waiting for {}", url),
        }
    }
}

async fn create_topic(config: &E2eConfig) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_bootstrap)
        .create()
        .context("failed to create Kafka admin client")?;

    let topic = NewTopic::new(&config.topic, 1, TopicReplication::Fixed(1));
    let result = admin
        .create_topics(&[topic], &AdminOptions::new())
        .await
        .context("failed to create Kafka topic")?;

    for item in result {
        match item {
            Ok(_) => {}
            Err((name, error)) if error.to_string().contains("already exists") => {
                println!("topic already exists: {}", name);
            }
            Err((name, error)) => bail!("failed to create topic {}: {}", name, error),
        }
    }

    Ok(())
}

async fn wait_for_rpc(config: &E2eConfig) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if config.rpc_socket.exists() && rpc_health(config).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for K2I RPC socket {}",
                config.rpc_socket.display()
            );
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn rpc_health(config: &E2eConfig) -> Result<()> {
    match rpc_request(config, RpcRequest::Health).await? {
        RpcResponse::Health { ok: true, .. } => Ok(()),
        other => bail!("unexpected RPC health response: {:?}", other),
    }
}

async fn rpc_request(config: &E2eConfig, request: RpcRequest) -> Result<RpcResponse> {
    let stream = UnixStream::connect(&config.rpc_socket)
        .await
        .with_context(|| format!("failed to connect {}", config.rpc_socket.display()))?;
    let mut framed = LengthDelimitedCodec::builder()
        .length_field_type::<u32>()
        .little_endian()
        .max_frame_length(config.max_frame_bytes)
        .new_framed(stream);

    framed
        .send(Bytes::from(encode_request(&request)?))
        .await
        .context("failed to send RPC request")?;
    let frame = framed
        .next()
        .await
        .ok_or_else(|| anyhow!("RPC connection closed before response"))?
        .context("failed to read RPC response")?;
    decode_response(&frame, config.max_frame_bytes).context("failed to decode RPC response")
}

async fn scan_begin(config: &E2eConfig) -> Result<ReadState> {
    match rpc_request(
        config,
        RpcRequest::ScanTableBegin {
            database: DATABASE.to_string(),
            table: TABLE.to_string(),
            lsn_bound: None,
        },
    )
    .await?
    {
        RpcResponse::ReadState(state) => {
            let scan_id = state.scan_id;
            let _ = rpc_request(
                config,
                RpcRequest::ScanTableEnd {
                    database: DATABASE.to_string(),
                    table: TABLE.to_string(),
                    scan_id,
                },
            )
            .await;
            Ok(state)
        }
        RpcResponse::Error(error) => bail!("scan failed: {}: {}", error.code, error.message),
        other => bail!("unexpected scan response: {:?}", other),
    }
}

async fn wait_for_state<F>(
    label: &str,
    config: &E2eConfig,
    timeout: Duration,
    mut predicate: F,
) -> Result<ReadState>
where
    F: FnMut(&ReadState) -> Result<bool>,
{
    let deadline = Instant::now() + timeout;
    let mut last_state: Option<ReadState>;
    loop {
        let state = scan_begin(config).await?;
        if predicate(&state)? {
            return Ok(state);
        }
        last_state = Some(state);
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for {}; last state: {:?}",
                label,
                last_state
            );
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn set_subject_compatibility_none(config: &E2eConfig) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/config/{}", config.schema_registry_url, SUBJECT);
    let response = client
        .put(url)
        .json(&json!({ "compatibility": "NONE" }))
        .send()
        .await
        .context("failed to set Schema Registry compatibility")?;
    if !response.status().is_success() {
        bail!(
            "failed to set Schema Registry compatibility: {} {}",
            response.status(),
            response.text().await.unwrap_or_default()
        );
    }
    Ok(())
}

async fn register_schema(config: &E2eConfig, schema: &'static str) -> Result<i32> {
    let client = reqwest::Client::new();
    let url = format!(
        "{}/subjects/{}/versions",
        config.schema_registry_url, SUBJECT
    );
    let response = client
        .post(url)
        .json(&json!({
            "schemaType": "PROTOBUF",
            "schema": schema,
        }))
        .send()
        .await
        .context("failed to register Protobuf schema")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!("schema registration failed with {}: {}", status, body);
    }
    let value: serde_json::Value =
        serde_json::from_str(&body).context("invalid Schema Registry register response")?;
    value
        .get("id")
        .and_then(|id| id.as_i64())
        .map(|id| id as i32)
        .ok_or_else(|| anyhow!("Schema Registry response did not include id: {}", body))
}

async fn produce_v1(
    config: &E2eConfig,
    schema_id: i32,
    laps: std::ops::RangeInclusive<i32>,
) -> Result<()> {
    let records = laps
        .map(|lap| {
            let msg = DerivedStateV1 {
                driver: format!("DRV{}", lap),
                lap,
                sector_time: 28.0 + f64::from(lap) / 100.0,
                telemetry: Some(Telemetry {
                    speed: 310.0 + f64::from(lap),
                    gear: 7,
                }),
                samples: vec![1.0, 2.0],
                counters: HashMap::from([("sector_1_ms".to_string(), 28_000 + lap)]),
                status_text: "green".to_string(),
            };
            (
                format!("driver-{}", lap),
                confluent_frame(schema_id, msg.encode_to_vec()),
            )
        })
        .collect::<Vec<_>>();
    produce_records(config, records).await
}

async fn produce_v2(
    config: &E2eConfig,
    schema_id: i32,
    laps: std::ops::RangeInclusive<i32>,
) -> Result<()> {
    let records = laps
        .map(|lap| {
            let msg = DerivedStateV2 {
                driver: format!("DRV{}", lap),
                lap,
                sector_time: 28.0 + f64::from(lap) / 100.0,
                telemetry: Some(Telemetry {
                    speed: 310.0 + f64::from(lap),
                    gear: 7,
                }),
                samples: vec![1.0, 2.0],
                counters: HashMap::from([("sector_1_ms".to_string(), 28_000 + lap)]),
                status_text: "green".to_string(),
                team: "Red Bull".to_string(),
            };
            (
                format!("driver-{}", lap),
                confluent_frame(schema_id, msg.encode_to_vec()),
            )
        })
        .collect::<Vec<_>>();
    produce_records(config, records).await
}

async fn produce_v3_breaking(config: &E2eConfig, schema_id: i32, lap: i32) -> Result<()> {
    let msg = DerivedStateV3Breaking {
        driver: format!("DRV{}", lap),
        lap: lap.to_string(),
        sector_time: 29.0,
    };
    produce_records(
        config,
        vec![(
            format!("driver-{}", lap),
            confluent_frame(schema_id, msg.encode_to_vec()),
        )],
    )
    .await
}

async fn produce_records(config: &E2eConfig, records: Vec<(String, Vec<u8>)>) -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_bootstrap)
        .set("message.timeout.ms", "10000")
        .create()
        .context("failed to create Kafka producer")?;

    for (key, payload) in records {
        producer
            .send(
                FutureRecord::to(&config.topic).key(&key).payload(&payload),
                Timeout::After(Duration::from_secs(10)),
            )
            .await
            .map_err(|(error, _)| anyhow!("failed to produce Kafka record: {}", error))?;
    }

    Ok(())
}

async fn produce_load_range(
    config: &E2eConfig,
    schema_id: i32,
    laps: std::ops::RangeInclusive<i32>,
    team: Option<&str>,
    concurrency: usize,
) -> Result<()> {
    if laps.is_empty() {
        return Ok(());
    }

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_bootstrap)
        .set("message.timeout.ms", "30000")
        .set("queue.buffering.max.messages", "1000000")
        .create()
        .context("failed to create Kafka load producer")?;

    let mut in_flight = FuturesUnordered::new();
    for lap in laps {
        let key = format!("driver-{}", lap);
        let payload = match team {
            Some(team) => {
                let msg = DerivedStateV2 {
                    driver: format!("DRV{}", lap),
                    lap,
                    sector_time: 28.0 + f64::from(lap.rem_euclid(100)) / 100.0,
                    telemetry: Some(Telemetry {
                        speed: 300.0 + f64::from(lap.rem_euclid(50)),
                        gear: 7,
                    }),
                    samples: vec![1.0, 2.0, 3.0],
                    counters: HashMap::from([("sector_1_ms".to_string(), 28_000 + lap)]),
                    status_text: "green".to_string(),
                    team: team.to_string(),
                };
                confluent_frame(schema_id, msg.encode_to_vec())
            }
            None => {
                let msg = DerivedStateV1 {
                    driver: format!("DRV{}", lap),
                    lap,
                    sector_time: 28.0 + f64::from(lap.rem_euclid(100)) / 100.0,
                    telemetry: Some(Telemetry {
                        speed: 300.0 + f64::from(lap.rem_euclid(50)),
                        gear: 7,
                    }),
                    samples: vec![1.0, 2.0, 3.0],
                    counters: HashMap::from([("sector_1_ms".to_string(), 28_000 + lap)]),
                    status_text: "green".to_string(),
                };
                confluent_frame(schema_id, msg.encode_to_vec())
            }
        };

        loop {
            let record = FutureRecord::to(&config.topic).key(&key).payload(&payload);
            match producer.send_result(record) {
                Ok(future) => {
                    in_flight.push(future);
                    break;
                }
                Err((error, _)) if error.to_string().contains("QueueFull") => {
                    sleep(Duration::from_millis(10)).await;
                }
                Err((error, _)) => bail!("failed to enqueue Kafka record: {}", error),
            }
        }

        if in_flight.len() >= concurrency {
            await_delivery(in_flight.next().await).await?;
        }
    }

    while !in_flight.is_empty() {
        await_delivery(in_flight.next().await).await?;
    }

    Ok(())
}

async fn await_delivery(
    result: Option<std::result::Result<OwnedDeliveryResult, futures::channel::oneshot::Canceled>>,
) -> Result<()> {
    let delivery = result
        .ok_or_else(|| anyhow!("load producer delivery stream ended unexpectedly"))?
        .context("Kafka delivery future was canceled")?;
    delivery
        .map(|_| ())
        .map_err(|(error, message)| anyhow!("Kafka delivery failed for {:?}: {}", message, error))
}

fn confluent_frame(schema_id: i32, payload: Vec<u8>) -> Vec<u8> {
    let mut frame = Vec::with_capacity(6 + payload.len());
    frame.push(0);
    frame.extend_from_slice(&schema_id.to_be_bytes());
    frame.push(0);
    frame.extend_from_slice(&payload);
    frame
}

fn visible_row_count(state: &ReadState) -> Result<u64> {
    Ok(cold_row_count(state) + hot_row_count(state)? as u64)
}

fn cold_row_count(state: &ReadState) -> u64 {
    state.data_files.iter().map(|file| file.row_count).sum()
}

fn hot_row_count(state: &ReadState) -> Result<usize> {
    let Some(bytes) = &state.hot_arrow_ipc else {
        return Ok(0);
    };
    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .context("failed to read hot Arrow IPC stream")?;
    let mut rows = 0;
    for batch in reader {
        rows += batch
            .context("failed to decode hot Arrow batch")?
            .num_rows();
    }
    Ok(rows)
}

fn assert_hot_rows(
    state: &ReadState,
    expected_rows: usize,
    expected_column: Option<&str>,
) -> Result<()> {
    let Some(bytes) = &state.hot_arrow_ipc else {
        bail!("expected hot Arrow IPC with {} rows", expected_rows);
    };
    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .context("failed to read hot Arrow IPC stream")?;
    let schema = reader.schema();
    if let Some(column) = expected_column {
        schema
            .field_with_name(column)
            .with_context(|| format!("hot Arrow IPC did not contain expected column {}", column))?;
    }

    let mut rows = 0;
    let mut saw_team_value = expected_column.is_none();
    for batch in reader {
        let batch = batch.context("failed to decode hot Arrow batch")?;
        rows += batch.num_rows();
        if expected_column == Some("team") {
            let team = batch
                .column_by_name("team")
                .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| anyhow!("team column was not Utf8"))?;
            saw_team_value |=
                (0..team.len()).any(|idx| !team.is_null(idx) && team.value(idx) == "Red Bull");
        }
    }

    if rows != expected_rows {
        bail!("expected {} hot rows, got {}", expected_rows, rows);
    }
    if !saw_team_value {
        bail!("team column did not contain expected Red Bull value");
    }
    Ok(())
}

fn duckdb_count(state: &ReadState) -> Result<u64> {
    let paths = state
        .data_files
        .iter()
        .map(|file| file.path.clone())
        .collect::<Vec<_>>();
    if paths.is_empty() {
        return Ok(0);
    }

    let script = r#"
import duckdb
import json
import os
paths = json.loads(os.environ["K2I_PARQUET_PATHS"])
quoted = ",".join("'" + path.replace("'", "''") + "'" for path in paths)
sql = "SELECT COUNT(*) FROM read_parquet([" + quoted + "], union_by_name=true)"
print(duckdb.connect().execute(sql).fetchone()[0])
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(script)
        .env("K2I_PARQUET_PATHS", serde_json::to_string(&paths)?)
        .output()
        .context("failed to run DuckDB Python validation")?;

    if !output.status.success() {
        bail!(
            "DuckDB validation failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    String::from_utf8(output.stdout)?
        .trim()
        .parse::<u64>()
        .context("DuckDB count output was not an integer")
}

fn duckdb_iceberg_count(warehouse_path: &Path) -> Result<u64> {
    let metadata_path = latest_iceberg_metadata_file(warehouse_path)?;
    let script = r#"
import duckdb
import os
path = os.environ["K2I_ICEBERG_METADATA_PATH"]
escaped = path.replace("'", "''")
conn = duckdb.connect()
conn.execute("INSTALL iceberg")
conn.execute("LOAD iceberg")
print(conn.execute("SELECT COUNT(*) FROM iceberg_scan('" + escaped + "')").fetchone()[0])
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(script)
        .env("K2I_ICEBERG_METADATA_PATH", metadata_path)
        .output()
        .context("failed to run DuckDB Iceberg validation")?;

    if !output.status.success() {
        bail!(
            "DuckDB Iceberg validation failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    String::from_utf8(output.stdout)?
        .trim()
        .parse::<u64>()
        .context("DuckDB Iceberg count output was not an integer")
}

fn assert_iceberg_metadata_files(warehouse_path: &Path) -> Result<()> {
    let mut metadata_json = Vec::new();
    let mut avro = Vec::new();
    collect_iceberg_metadata_files(warehouse_path, &mut metadata_json, &mut avro)?;

    if metadata_json.is_empty() {
        bail!(
            "expected Iceberg metadata JSON under {}",
            warehouse_path.display()
        );
    }
    if avro.len() < 2 {
        bail!(
            "expected manifest list and manifest Avro files under {}, found {}",
            warehouse_path.display(),
            avro.len()
        );
    }
    Ok(())
}

fn latest_iceberg_metadata_file(warehouse_path: &Path) -> Result<String> {
    let mut metadata_json = Vec::new();
    let mut avro = Vec::new();
    collect_iceberg_metadata_files(warehouse_path, &mut metadata_json, &mut avro)?;
    metadata_json
        .into_iter()
        .max_by_key(|path| {
            fs::metadata(path)
                .and_then(|metadata| metadata.modified())
                .ok()
        })
        .map(|path| path.to_string_lossy().to_string())
        .ok_or_else(|| {
            anyhow!(
                "no Iceberg metadata JSON files found under {}",
                warehouse_path.display()
            )
        })
}

fn iceberg_snapshot_count(warehouse_path: &Path) -> Result<usize> {
    let metadata_path = latest_iceberg_metadata_file(warehouse_path)?;
    let body = fs::read_to_string(&metadata_path)
        .with_context(|| format!("failed to read Iceberg metadata {}", metadata_path))?;
    let value: serde_json::Value = serde_json::from_str(&body)
        .with_context(|| format!("failed to parse Iceberg metadata {}", metadata_path))?;
    value
        .get("snapshots")
        .and_then(|snapshots| snapshots.as_array())
        .map(|snapshots| snapshots.len())
        .ok_or_else(|| {
            anyhow!(
                "Iceberg metadata {} did not contain snapshots",
                metadata_path
            )
        })
}

fn collect_iceberg_metadata_files(
    dir: &Path,
    metadata_json: &mut Vec<PathBuf>,
    avro: &mut Vec<PathBuf>,
) -> Result<()> {
    for entry in fs::read_dir(dir)
        .with_context(|| format!("failed to read warehouse directory {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_iceberg_metadata_files(&path, metadata_json, avro)?;
            continue;
        }

        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.ends_with(".metadata.json") {
            metadata_json.push(path);
        } else if name.ends_with(".avro") {
            avro.push(path);
        }
    }
    Ok(())
}

fn assert_duckdb_team_values(state: &ReadState) -> Result<()> {
    let paths = state
        .data_files
        .iter()
        .map(|file| file.path.clone())
        .collect::<Vec<_>>();
    if paths.is_empty() {
        bail!("expected Parquet files for DuckDB team validation");
    }

    let script = r#"
import duckdb
import json
import os
paths = json.loads(os.environ["K2I_PARQUET_PATHS"])
quoted = ",".join("'" + path.replace("'", "''") + "'" for path in paths)
sql = (
    "SELECT COUNT(*) AS total, "
    "COUNT(*) FILTER (WHERE team IS NULL) AS null_team, "
    "COUNT(*) FILTER (WHERE team = 'Red Bull') AS red_bull "
    "FROM read_parquet([" + quoted + "], union_by_name=true)"
)
print(json.dumps(duckdb.connect().execute(sql).fetchone()))
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(script)
        .env("K2I_PARQUET_PATHS", serde_json::to_string(&paths)?)
        .output()
        .context("failed to run DuckDB team validation")?;

    if !output.status.success() {
        bail!(
            "DuckDB team validation failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let counts: Vec<u64> =
        serde_json::from_slice(&output.stdout).context("invalid DuckDB team counts")?;
    if counts.as_slice() != [10, 5, 5] {
        bail!(
            "unexpected DuckDB team counts; expected [10, 5, 5], got {:?}",
            counts
        );
    }
    Ok(())
}

async fn assert_schema_degraded(config: &E2eConfig) -> Result<()> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let response = client
            .get(format!("{}/health", config.health_url))
            .send()
            .await
            .context("failed to query K2I health")?;
        let value: serde_json::Value = response
            .json()
            .await
            .context("failed to decode K2I health response")?;
        let schema = &value["components"]["schema"];
        let status = schema["status"].as_str();
        let message = schema["message"].as_str().unwrap_or_default();
        if status == Some("degraded")
            && message.contains("breaking schema change")
            && message.contains("lap")
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "schema health did not become degraded; last health: {}",
                value
            );
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn assert_readyz_blocked(config: &E2eConfig) -> Result<()> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let response = client
            .get(format!("{}/readyz", config.health_url))
            .send()
            .await
            .context("failed to query K2I readiness")?;
        if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "readyz did not become blocked; last status {}",
                response.status()
            );
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn assert_metrics_errors_zero(config: &E2eConfig) -> Result<()> {
    let body = reqwest::get(format!("{}/metrics", config.metrics_url))
        .await
        .context("failed to query K2I metrics")?
        .error_for_status()
        .context("K2I metrics endpoint returned an error status")?
        .text()
        .await
        .context("failed to read K2I metrics")?;
    for line in body.lines() {
        if let Some(value) = line.strip_prefix("k2i_errors_total ") {
            let errors: u64 = value
                .trim()
                .parse()
                .context("k2i_errors_total was not an integer")?;
            if errors != 0 {
                bail!("expected k2i_errors_total 0, got {}", errors);
            }
            return Ok(());
        }
    }
    bail!("k2i_errors_total was not present in metrics output")
}

fn schema_v1() -> &'static str {
    r#"syntax = "proto3";
package f1.timing.v1;
message DerivedState {
  string driver = 1;
  int32 lap = 2;
  double sector_time = 3;
  Telemetry telemetry = 4;
  repeated double samples = 5;
  map<string, int32> counters = 6;
  oneof status {
    string status_text = 7;
    int32 status_code = 8;
  }
  message Telemetry {
    double speed = 1;
    int32 gear = 2;
  }
}
"#
}

fn schema_v2() -> &'static str {
    r#"syntax = "proto3";
package f1.timing.v1;
message DerivedState {
  string driver = 1;
  int32 lap = 2;
  double sector_time = 3;
  Telemetry telemetry = 4;
  repeated double samples = 5;
  map<string, int32> counters = 6;
  oneof status {
    string status_text = 7;
    int32 status_code = 8;
  }
  string team = 9;
  message Telemetry {
    double speed = 1;
    int32 gear = 2;
  }
}
"#
}

fn schema_v3_breaking() -> &'static str {
    r#"syntax = "proto3";
package f1.timing.v1;
message DerivedState {
  string driver = 1;
  string lap = 2;
  double sector_time = 3;
}
"#
}
