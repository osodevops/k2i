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
| `K2I_MONITORING_OTEL_CORRELATE_KAFKA_TRACE_CONTEXT` | `monitoring.otel.correlate_kafka_trace_context` |

When `enabled` is false, K2I builds no exporter and the ingestion path creates
no spans; the per-step cost is one relaxed atomic load.

## Spans

| Span | Parent | Attributes |
|---|---|---|
| `k2i.poll` | — | `topic`, `batch_size`, `partition_count` |
| `k2i.message` | the producer's span (when correlated) | `topic`, `partition`, `offset` |
| `k2i.decode` | `k2i.poll` | `format`, `row_count`, `dropped_trace_links` |
| `k2i.flush` | — | `batch_id`, `row_count`, `file_size_bytes`, `dropped_trace_links` |
| `k2i.iceberg.commit` | `k2i.flush` | `database`, `table`, `snapshot_id`, `dropped_trace_links` |
| `k2i.schema.resolve` | — | `subject`, `strategy` |
| `k2i.catalog.health` | — | `catalog_type` |

What each one covers:

- **`k2i.poll`** — one Kafka `poll_batch` call, including wait time. An empty
  poll creates no span. `partition_count` is the number of distinct partitions
  in the returned batch.
- **`k2i.message`** — one consumed record that carried a producer
  `traceparent` header. It is a child of the producer's span, so the record
  shows up inside the producer's trace. Only created when tracing and
  correlation are on and the header is valid and sampled.
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
shows the path from an ingest to the file it produced. Without producer
correlation (see below) every poll starts a new trace.

## Correlating with the producer's trace

K2I reads the W3C Trace Context (`traceparent`, and `tracestate` when present)
from the headers of each consumed Kafka record and uses it **only when the
producer put it there**. K2I cannot invent the producer's span: if the
producer does not inject `traceparent`, K2I's spans stand alone exactly as
described above and correlation adds nothing.

The switch is `monitoring.otel.correlate_kafka_trace_context`, on by default.
It has no effect while `monitoring.otel.enabled` is false: headers are not
read, no spans are created, and no per-message state is stored.

```toml
[monitoring.otel]
enabled = true
endpoint = "http://otel-collector:4317"
service_name = "k2i"
correlate_kafka_trace_context = true
```

### What the producer must do

Inject the standard `traceparent` header (and optionally `tracestate`) into
the Kafka record headers from the producer's current span. K2I accepts any
W3C-valid, **sampled** value (`trace-flags` bit 0 set); a missing, empty,
malformed, binary, or unsampled value is treated as "no parent" and never
fails ingestion. Header names are matched case-insensitively.

Common producers:

- **Java / `opentelemetry-java`** — the Kafka instrumentation adds the headers
  automatically; otherwise inject with
  `W3CTraceContextPropagator.getInstance().inject(Context.current(), record.headers(), (carrier, key, value) -> carrier.add(key, value.getBytes(StandardCharsets.UTF_8)))`.
- **Go / `otelkafka` or manual** — build a `propagation.HeaderCarrier`, call
  `otel.GetTextMapPropagator().Inject(ctx, carrier)`, and append each carrier
  entry as a Kafka header.
- **Python / `opentelemetry-instrumentation-kafka-python`** — enabled by
  `KafkaInstrumentor().instrument()`; the injected header is `traceparent`.
- **Any language or a hand-rolled producer** — produce the header yourself:
  `traceparent: 00-<32 hex trace id>-<16 hex span id>-01` where the trace and
  span ids come from the producer's active span.

### Parent and link model

A batch mixes messages from different producers, so there is no single
honest parent for the batch:

- Each traced message gets its own **`k2i.message` span**, parented to the
  producer's span. This is what makes the producer's trace show "K2I consumed
  this record".
- The batch's **`k2i.decode`** span stays parented to `k2i.poll` and **links**
  to the message spans it decoded.
- **`k2i.flush`** (and its `k2i.iceberg.commit` child) happens later, on rows
  that may come from many producers, so it **links** to the message spans of
  the rows it drained. Nesting it under one producer would attribute the
  whole file to that producer and misrepresent the others.

Links are capped at 32 distinct traces per span. Duplicates from the same
trace collapse into one link, and traces that did not fit are counted in the
span's `dropped_trace_links` attribute, so you can see when a span's link set
was truncated.

The per-row link is a compact 16-byte trace id plus 8-byte span id kept in the
hot buffer, and only for rows whose message carried a context. With
correlation off (or no producer injecting `traceparent`) the buffer stores
nothing extra, no per-message allocation is made, and no Arrow column is added.

### What an operator sees

| Producer | In the trace backend |
|---|---|
| Traced and sampled | The producer's trace contains the producer's span, a `k2i.message` child per consumed record, and `k2i.decode`, `k2i.flush`, and `k2i.iceberg.commit` linked from the batch spans. Searching the producer's trace id shows the row's path to the Iceberg file. |
| Traced but not sampled (`-00`) | K2I ignores the context and creates no `k2i.message` span; K2I's own spans appear as a separate trace with no links. This matches the W3C sampling contract. |
| Not traced | K2I's spans stand alone: `k2i.poll` → `k2i.decode` and `k2i.flush` → `k2i.iceberg.commit`, with no `k2i.message` span and no links. |

A trace backend that displays links (Jaeger, Tempo/Grafana) shows them as
"follows from" edges; `k2i.flush` will fan in from every traced producer whose
rows it flushed.

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
- **No `k2i.message` spans and no links** — the producer is not injecting
  `traceparent`, or it injects an unsampled/uppercase/malformed value. Inspect
  a consumed record's headers with `kafka-console-consumer` or your producer's
  instrumentation; K2I silently treats anything invalid as "no parent".
- **Links exist but `dropped_trace_links` is non-zero** — more than 32 distinct
  producer traces fed one batch. The first 32 are linked; the count tells you
  how many were not.
