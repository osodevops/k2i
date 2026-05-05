# K2I README And Docs SEO Plan

Date: 2026-05-05

This plan uses the copied Codex `seo-research` skill workflow manually: Perplexity discovery for market/search intent and SEMrush API checks for keyword demand. Raw SEMrush exports are in `seo-research/semrush-data/`.

## Executive Recommendation

Position K2I as:

> Kafka to Apache Iceberg ingestion in one Rust binary.

Use the phrase **Kafka to Iceberg** in the README title, opening paragraph, docs index, and release notes. It is low-volume but bottom-of-funnel and low-difficulty in SEMrush, which makes it a good GitHub/repo discovery target.

Do not position K2I as a Kafka Connect plugin, Flink replacement, CDC system, or managed TableFlow equivalent. The accurate wedge is:

- standalone Kafka consumer/service, not Kafka Connect;
- single configured topic/table per process today;
- best for final-form Kafka events that should become Iceberg/Parquet analytics data;
- simpler operational surface than Flink/Spark/Kafka Connect clusters for this narrow job;
- local hot read-state path for fresh rows, plus cold Iceberg commits for analytics engines.

## Research Findings

### SEMrush Signal

Exact Kafka-to-Iceberg terms are niche, but commercial/implementation intent is strong:

| Keyword | US Volume | KD | Intent Notes | Content Target |
|---|---:|---:|---|---|
| `kafka to iceberg` | 20 | 0 | exact bottom-of-funnel | README H1/opening, docs landing |
| `kafka iceberg sink` | 20 | 0 | connector/sink comparison intent | comparison section |
| `kafka connect iceberg sink` | 20 | 0 | alternative/implementation intent | comparison page |
| `iceberg streaming ingestion` | 20 | 0 | architecture intent | architecture/docs landing |
| `flink iceberg sink` | 20 | 0 | alternative intent | vs Flink comparison |
| `kafka to parquet` | 20 | 0 | adjacent sink/file-format intent | Parquet write docs |

Adjacent topics have larger discovery demand and should be used in docs headings and internal links:

| Keyword | US Volume | KD | Why It Matters |
|---|---:|---:|---|
| `apache parquet` | 8100 | 51 | K2I writes Parquet data files |
| `apache iceberg` | 6600 | 61 | broad category awareness |
| `apache flink` | 5400 | 69 | common alternative |
| `data lakehouse` | 2400 | 40 | architectural discovery |
| `kafka connect` | 1600 | 52 | common alternative infrastructure |
| `parquet file format` | 1600 | 47 | format education |
| `confluent schema registry` | 390 | 39 | Protobuf ingest proof point |
| `kafka schema registry` | 390 | 25 | same intent, lower KD |
| `duckdb iceberg` | 210 | 10 | K2I validation and local examples |
| `iceberg rest catalog` | 210 | 8 | K2I REST catalog integration |
| `confluent tableflow` | 210 | 15 | managed alternative |
| `kafka to s3` | 140 | 9 | adjacent ingestion intent |
| `iceberg rest api` | 70 | 44 | catalog/API reference |
| `duckdb iceberg rest catalog` | 50 | 33 | niche but directly relevant |
| `small file problem` | 30 | 0 | problem-language hook |

### Perplexity Search Intent

Recurring intent clusters:

- **How do I stream Kafka data into Iceberg?**
- **Should I use Kafka Connect, Flink, Spark, or a managed service?**
- **How do I avoid the Iceberg small-file problem with streaming writes?**
- **Can Schema Registry changes become Iceberg schema changes safely?**
- **How do I verify an Iceberg table locally with DuckDB?**
- **What does exactly-once mean across Kafka offsets, object storage, and Iceberg commits?**

Frequently cited options and reference entities:

- Apache Iceberg official docs and REST catalog spec.
- Kafka Connect Iceberg sink docs.
- Flink Iceberg sink and dynamic sink materials.
- Confluent TableFlow and managed Kafka-to-Iceberg products.
- AWS/MSK Connect and Glue examples.
- DuckDB Iceberg extension docs.
- Streambased and AutoMQ comparison articles around the Kafka/Iceberg file-size mismatch.

Useful source anchors for docs:

- Apache Iceberg Kafka Connect docs: https://iceberg.apache.org/docs/nightly/kafka-connect/
- Apache Iceberg REST catalog spec: https://iceberg.apache.org/rest-catalog-spec/
- DuckDB Iceberg extension docs: https://duckdb.org/docs/current/core_extensions/iceberg/overview.html
- Streambased Kafka-to-Iceberg comparison: https://blog.streambased.io/p/the-9-ways-to-move-data-kafka-iceberg
- AutoMQ Kafka-to-Iceberg comparison: https://www.automq.com/blog/kafka-to-iceberg-top-9-ways-2026
- Confluent TableFlow product page: https://www.confluent.io/product/tableflow/

## Claim Discipline

The release docs should be more precise than the current README in a few places.

| Current/Tempting Claim | Safer Release Wording |
|---|---|
| `production-grade streaming ingestion` | `production-oriented Kafka-to-Iceberg ingestion engine` until caveats are closed |
| `exactly-once semantics` | `designed for exactly-once-style durability through manual Kafka offsets, write-ahead transaction log records, idempotency records, and atomic Iceberg commits` |
| `sub-second query freshness` | `sub-second hot-path visibility through the local read-state RPC; cold Iceberg visibility depends on flush and catalog commit timing` |
| `JSON ingest` | `raw bytes and JSON-compatible raw payloads today; typed Protobuf decoding through Schema Registry` |
| `GCS/Azure support` | `S3/local filesystem writer paths today; GCS/Azure declared in configuration but writer wiring is not complete` |
| `automated maintenance` | `maintenance commands and task implementations exist; production scheduler wiring should be reviewed per deployment` |
| `validated with DuckDB` | `validated locally with Docker using DuckDB direct Parquet reads and DuckDB iceberg_scan against real Iceberg REST metadata` |

## README Plan

### H1 And Tagline

Recommended:

```markdown
# K2I: Kafka to Apache Iceberg in One Rust Binary

Stream final-form Kafka events into Apache Iceberg tables with Protobuf Schema Registry decoding, Arrow hot reads, Parquet writes, and Docker-verified DuckDB/Iceberg validation.
```

### Opening Paragraph

Use this shape:

```markdown
K2I is an open-source, standalone Kafka-to-Iceberg ingestion engine. It consumes a Kafka topic, decodes raw or Confluent-framed Protobuf messages, keeps recent rows visible through an Arrow-backed hot read path, and flushes Parquet data files through Iceberg catalog commits. It is built for teams that want fresh lakehouse tables without operating a Flink job, Spark micro-batch pipeline, or Kafka Connect cluster for a simple final-form event stream.
```

### Above-The-Fold Structure

1. Badges.
2. One-line description with `Kafka to Apache Iceberg`.
3. Three-line “Use K2I when...” fit statement.
4. Quick Docker E2E command.
5. Feature list ordered by search intent:
   - Kafka to Iceberg ingestion.
   - Protobuf Schema Registry and additive schema evolution.
   - Arrow hot read-state RPC.
   - Parquet + Iceberg REST metadata commits.
   - WAL/idempotency/recovery design.
   - DuckDB/Docker E2E verification.
   - Metrics, health, maintenance, man pages.

### README Sections To Add Or Rework

- `What K2I Is`: standalone service, one Kafka topic/table per process today.
- `When To Use K2I`: final-form Kafka events, low-ops ingestion, local read freshness, Iceberg lakehouse writes.
- `When Not To Use K2I`: complex transforms, joins/windows, CDC deletes/upserts, multi-source ETL, remote public read serving.
- `K2I vs Alternatives`: comparison table for Kafka Connect Iceberg sink, Flink Iceberg sink, Spark micro-batch, Confluent TableFlow, Moonlink.
- `What Is Validated`: unit tests, clippy/fmt/check, Docker E2E, 100k-row Iceberg load, DuckDB `iceberg_scan`.
- `Known Release Caveats`: short honest list linking to production readiness.
- `FAQ`: concise LLM-friendly questions.

## Docs Plan

### Keep And Tighten Existing Docs

| Existing File | Action |
|---|---|
| `docs/README.md` | Reframe as documentation hub for `Kafka to Iceberg` and link new comparison/tutorial pages |
| `docs/quickstart.md` | Start with Docker E2E path and explicitly show DuckDB/Iceberg validation |
| `docs/configuration.md` | Add search-friendly anchors for Schema Registry, Iceberg REST catalog, S3/local filesystem |
| `docs/architecture.md` | Add “Kafka small messages to Iceberg data files” framing and clarify hot vs cold freshness |
| `docs/commands.md` | Keep CLI/man-page coverage; add `completions man` in release checklist |
| `docs/production-readiness.md` | Keep caveats visible; use this as the claims backstop |

### Add New SEO/LLM-Oriented Docs

Recommended docs to create:

1. `docs/kafka-to-iceberg.md`
   - Target: `kafka to iceberg`, `kafka iceberg sink`, `iceberg streaming ingestion`.
   - Explain the problem, K2I fit, architecture, examples, limitations.

2. `docs/comparisons.md`
   - Target: `kafka connect iceberg sink`, `flink iceberg sink`, `confluent tableflow`.
   - Decision table only; avoid competitor-bashing.

3. `docs/duckdb-iceberg-validation.md`
   - Target: `duckdb iceberg`, `duckdb iceberg rest catalog`.
   - Show the Docker E2E and why `iceberg_scan` matters.

4. `docs/schema-registry-protobuf.md`
   - Target: `confluent schema registry`, `kafka schema registry`, `protobuf schema registry`.
   - Explain Confluent framing, additive evolution, breaking-change pause behavior.

5. `docs/iceberg-rest-catalog.md`
   - Target: `iceberg rest catalog`, `iceberg rest api`.
   - Explain current REST path and how it is validated locally.

6. `docs/faq.md`
   - Target: question/LLM extraction.
   - Keep answers short and link deeper docs.

### FAQ Questions

Use these in README and `docs/faq.md`:

- What is K2I?
- How is K2I different from a Kafka Connect Iceberg sink?
- How is K2I different from Flink's Iceberg sink?
- How is K2I different from Confluent TableFlow?
- Is K2I a CDC tool like Moonlink?
- Does K2I guarantee exactly-once delivery?
- What happens if K2I crashes during a flush?
- How fresh is data in K2I?
- What does the hot read-state RPC expose?
- Can DuckDB read the Iceberg tables written by K2I?
- Which Iceberg catalogs does K2I support?
- How does K2I handle Protobuf schema evolution?
- What schema changes are rejected?
- Can K2I auto-create or reset a table locally?
- What is production-ready today and what needs hardening?

## Suggested Comparison Table

| Dimension | K2I | Kafka Connect Iceberg Sink | Flink Iceberg Sink | Spark Micro-Batch | Confluent TableFlow | Moonlink |
|---|---|---|---|---|---|---|
| Primary fit | final-form Kafka events to Iceberg | connector-based ingestion | stream processing and transforms | batch/micro-batch ETL | managed Confluent pipeline | Postgres CDC to Iceberg |
| Deployment | single Rust binary/container | Kafka Connect cluster | Flink cluster | Spark runtime | managed service | service/extension stack |
| Transformations | intentionally minimal | SMT/basic connector config | strong | strong | limited/managed | CDC-focused |
| Hot reads | local Arrow read-state RPC | no | no native local hot path | no | no local hot path | yes, CDC-oriented |
| Schema path | Confluent Protobuf additive evolution | connector/schema dependent | engine dependent | job dependent | managed | CDC/schema dependent |
| Best when | events are already analytics-shaped | you already run Connect | you need joins/windows/state | batch jobs are acceptable | you use Confluent Cloud | source is Postgres |
| Avoid when | you need joins/windows/CDC deletes | you do not want Connect ops | you want simple ingestion only | you need low latency | you need OSS/self-hosted | source is Kafka-only |

## Implementation Order

1. Rewrite README hero, opening, feature order, fit/avoid sections, validation section, and caveat wording.
2. Add `docs/kafka-to-iceberg.md` as the main SEO landing doc.
3. Add `docs/comparisons.md`.
4. Add `docs/duckdb-iceberg-validation.md`.
5. Add `docs/schema-registry-protobuf.md` and `docs/iceberg-rest-catalog.md`.
6. Add `docs/faq.md` and link it from README/docs index.
7. Update `docs/README.md`, `docs/quickstart.md`, `docs/architecture.md`, and `docs/production-readiness.md` links.
8. Regenerate man pages only if CLI help changes.
9. Run docs checks plus the standard cargo verification.

## Release Positioning

Recommended release framing:

> K2I is ready for a first public release as a production-oriented Kafka-to-Iceberg ingestion engine with strong local validation, Docker E2E coverage, real Iceberg REST metadata commits, and DuckDB `iceberg_scan` verification. The release notes should call out remaining hardening areas around multi-partition commit semantics, startup recovery application, async Kafka commit acknowledgement, per-entry fsync behavior, GCS/Azure writer wiring, and maintenance scheduler wiring.

This is a stronger trust signal than claiming broad “production ready” without caveats.
