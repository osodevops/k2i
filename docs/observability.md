# Observability

K2I has three independent observability surfaces:

| Surface | What it answers | Where it goes |
|---|---|---|
| Structured logs (`tracing` → JSON/text) | What happened, in order, per process | stdout |
| Prometheus metrics | How much and how fast, over time | `/metrics` |
| OpenTelemetry traces (optional) | Which ingest, flush, and catalog commit belong together, and where time went | OTLP/gRPC collector |

Traces and metrics coexist. With tracing off (the default) the JSON/text log
output and the metric names are unchanged. With tracing on, log lines keep
their format, and events emitted inside a span additionally carry that span's
fields (`span` / `spans`) — the same behaviour the JSON formatter already has
for any entered span.

## Enable tracing

Tracing is off by default. Turn it on in the `[monitoring]` block:

```toml
[monitoring.otel]
enabled = true
endpoint = "http://otel-collector:4317"   # OTLP/gRPC, usually port 4317
service_name = "k2i"
```

`endpoint` must be an `http://` or `https://` URL with a host; `k2i validate`
rejects anything else. The exporter does not connect during startup, so a
collector that is not up yet does not stop ingestion — spans are dropped until
it is reachable.

The same settings can be overridden by environment variables, which is the
usual way to enable tracing per environment:

| Variable | Config field |
|---|---|
| `K2I_MONITORING_OTEL_ENABLED` | `monitoring.otel.enabled` (`true`/`1`/`yes`/`on`) |
| `K2I_MONITORING_OTEL_ENDPOINT` | `monitoring.otel.endpoint` |
| `K2I_MONITORING_OTEL_SERVICE_NAME` | `monitoring.otel.service_name` |

When `enabled` is false, K2I builds no exporter and the ingestion path creates
no spans; the per-step cost is one relaxed atomic load.

## Spans

| Span | Parent | Attributes |
|---|---|---|
| `k2i.poll` | — | `topic`, `batch_size`, `partition_count` |
| `k2i.decode` | `k2i.poll` | `format`, `row_count` |
| `k2i.flush` | — | `batch_id`, `row_count`, `file_size_bytes` |
| `k2i.iceberg.commit` | `k2i.flush` | `database`, `table`, `snapshot_id` |
| `k2i.schema.resolve` | — | `subject`, `strategy` |
| `k2i.catalog.health` | — | `catalog_type` |

What each one covers:

- **`k2i.poll`** — one Kafka `poll_batch` call, including wait time. An empty
  poll creates no span. `partition_count` is the number of distinct partitions
  in the returned batch.
- **`k2i.decode`** — decoding that batch into an Arrow record batch. `format`
  is the decoder name (`raw` or `protobuf`).
- **`k2i.flush`** — one hot-buffer snapshot flushed to Parquet and committed.
  `batch_id` is the read-registry flush id, `file_size_bytes` the Parquet file
  size after the write. Spans are also created for the shutdown flush.
- **`k2i.iceberg.commit`** — the writer call that converts the snapshot to
  Parquet, uploads it, and commits catalog metadata; `snapshot_id` is the
  Iceberg snapshot id. The span covers the whole writer call, not only the
  catalog RPC.
- **`k2i.schema.resolve`** — startup resolution of the configured Protobuf
  subject (`latest_on_startup`). It is not created for raw/JSON formats or when
  startup resolution is disabled.
- **`k2i.catalog.health`** — the startup catalog health check, when a REST
  catalog is configured.

Because spans carry the Kafka topic and the Iceberg database/table, one trace
shows the path from an ingest to the file it produced. Trace context is not
propagated through Kafka message headers; every poll starts a new trace.

## Collector and Jaeger example

A minimal local stack: Jaeger all-in-one accepts OTLP directly, and K2I points
at it.

```yaml
# docker-compose.yml
services:
  jaeger:
    image: jaegertracing/all-in-one:1.62
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
    ports:
      - "16686:16686"   # Jaeger UI
      - "4317:4317"     # OTLP/gRPC

  k2i:
    image: ghcr.io/osodevops/k2i:latest
    command: ["ingest", "--config", "/etc/k2i/config.toml"]
    environment:
      K2I_MONITORING_OTEL_ENABLED: "true"
      K2I_MONITORING_OTEL_ENDPOINT: "http://jaeger:4317"
      K2I_MONITORING_OTEL_SERVICE_NAME: "k2i"
```

Open <http://localhost:16686> and select the `k2i` service. A flush appears as
`k2i.flush` with `k2i.iceberg.commit` nested below it; a poll appears as
`k2i.poll` with `k2i.decode` nested below it.

To route through a collector (recommended for production, since it can batch,
retry, and fan out), run the OpenTelemetry Collector with an OTLP receiver and
an OTLP exporter, and point K2I at the collector:

```yaml
# otel-collector.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

processors:
  batch: {}

exporters:
  otlp:
    endpoint: tempo:4317
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp]
```

Swap the exporter for `otlphttp` with a Tempo, Grafana Cloud, Datadog, or
Honeycomb endpoint as needed.

## Shutdown

On a graceful stop, K2I flushes the batch span processor before exiting, so the
last flush and commit spans are not lost. A hard kill (`SIGKILL`, pod eviction)
can lose spans buffered by the batch processor; this does not affect durability
of the ingested data, which is governed by the transaction log.

## Relationship to Prometheus

Prometheus remains the source for rates, totals, and alerting
(`k2i_messages_consumed_total`, `k2i_flushes_total`,
`k2i_errors_total`, `k2i_flush_duration_seconds`, …). Traces answer
per-request questions: which flush was slow, which catalog commit failed, and
which Kafka poll produced the rows in a file. Use the same `service_name` for
both so dashboards and traces line up.

## Troubleshooting

- **No spans in the collector** — check `k2i validate --config <file>` accepts
  the endpoint, that the process log contains `OpenTelemetry trace export
  enabled`, and that the collector listens on the OTLP/gRPC port (`4317` by
  default), not the HTTP port (`4318`).
- **Startup says `OpenTelemetry tracing disabled: …`** — the exporter could not
  be built; the message contains the reason, and ingestion continues without
  tracing so logging is still available.
- **Spans arrive a little late** — the batch processor buffers spans and
  exports them in batches, and flushes the remainder on graceful shutdown. This
  is expected; spans do not appear the instant the operation ends.
