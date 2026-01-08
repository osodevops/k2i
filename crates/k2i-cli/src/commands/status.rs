//! Status command implementation.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;

/// Health response from the server.
#[derive(Debug, Deserialize)]
struct HealthResponse {
    status: String,
    components: HashMap<String, ComponentStatus>,
}

/// Component status from the server.
#[derive(Debug, Deserialize)]
struct ComponentStatus {
    status: String,
    message: Option<String>,
}

/// Show status and health.
pub async fn run(url: &str) -> Result<()> {
    println!("Checking health at {}...\n", url);

    // Fetch health status
    let health_url = format!("{}/health", url.trim_end_matches('/'));
    let client = reqwest::Client::new();

    match client.get(&health_url).send().await {
        Ok(response) => {
            let status_code = response.status();
            match response.json::<HealthResponse>().await {
                Ok(health) => {
                    print_health_status(&health, status_code.as_u16());
                }
                Err(e) => {
                    println!("Failed to parse health response: {}", e);
                    println!("HTTP Status: {}", status_code);
                }
            }
        }
        Err(e) => {
            println!("Failed to connect to health endpoint: {}", e);
            println!("\nIs the k2i ingestion service running?");
            println!("Start it with: k2i ingest --config <path-to-config>");
            return Ok(());
        }
    }

    // Fetch metrics
    let metrics_url = url
        .replace(":8080", ":9090")
        .replace("/health", "")
        .trim_end_matches('/')
        .to_string()
        + "/metrics";

    println!("\n--- Metrics ---");
    println!("Fetching from {}...\n", metrics_url);

    match client.get(&metrics_url).send().await {
        Ok(response) => {
            let text = response.text().await.context("Failed to read metrics")?;
            print_metrics(&text);
        }
        Err(e) => {
            println!("Failed to fetch metrics: {}", e);
        }
    }

    Ok(())
}

fn print_health_status(health: &HealthResponse, status_code: u16) {
    let status_emoji = match health.status.as_str() {
        "healthy" => "[OK]",
        "degraded" => "[WARN]",
        "unhealthy" => "[FAIL]",
        _ => "[?]",
    };

    println!(
        "{} Overall Status: {} (HTTP {})",
        status_emoji,
        health.status.to_uppercase(),
        status_code
    );
    println!();

    if !health.components.is_empty() {
        println!("Components:");
        for (name, status) in &health.components {
            let emoji = match status.status.as_str() {
                "healthy" => "[OK]",
                "degraded" => "[WARN]",
                "unhealthy" => "[FAIL]",
                _ => "[?]",
            };

            if let Some(msg) = &status.message {
                println!("  {} {}: {} ({})", emoji, name, status.status, msg);
            } else {
                println!("  {} {}: {}", emoji, name, status.status);
            }
        }
    }
}

fn print_metrics(metrics_text: &str) {
    // Parse Prometheus format and display nicely
    let mut values: HashMap<&str, u64> = HashMap::new();

    for line in metrics_text.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }

        // Parse "metric_name value" format
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            if let Ok(value) = parts[1].parse::<u64>() {
                values.insert(parts[0], value);
            }
        }
    }

    // Display formatted metrics
    if let Some(v) = values.get("k2i_messages_consumed_total") {
        println!("Messages Consumed: {}", format_number(*v));
    }
    if let Some(v) = values.get("k2i_rows_flushed_total") {
        println!("Rows Flushed:      {}", format_number(*v));
    }
    if let Some(v) = values.get("k2i_flushes_total") {
        println!("Buffer Flushes:    {}", v);
    }
    if let Some(v) = values.get("k2i_errors_total") {
        println!("Errors:            {}", v);
    }
    if let Some(v) = values.get("k2i_backpressure_events_total") {
        println!("Backpressure Events: {}", v);
    }

    if values.is_empty() {
        println!("No metrics available yet.");
    }
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.2}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.2}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}
