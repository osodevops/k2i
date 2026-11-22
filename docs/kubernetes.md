# Deploying K2I on Kubernetes

K2I reads configuration from a TOML file and layers two secret-injection
mechanisms on top, so no secret ever needs to live in a ConfigMap:

1. **Secret file refs** — `{ file = "path" }` inline tables in TOML, designed
   for Kubernetes projected volumes and the Secrets Store CSI Driver.
2. **`K2I_*` environment variables** — override any field at runtime, designed
   for `env` / `envFrom` injection from `Secret` resources.

Storage volumes for durable state are covered in [State
persistence](#state-persistence).

Precedence (highest wins):

1. `K2I_*` environment variables
2. Inline TOML values (including `{ file = ... }` refs)

Invalid numeric/enum env values are rejected with a warning (the TOML or
default value is preserved), and unrecognized `K2I_*` variables are logged so
typos do not fail silently.

## State persistence

K2I writes durable state to disk. On Kubernetes, mount each path below on the
storage that matches its role. The image creates `/var/lib/k2i/txlog`, but its
contents only survive a pod restart if a volume is mounted there.

| Path (container) | Config key | Purpose | Persistence | Recommended volume | If lost |
|---|---|---|---|---|---|
| `/var/lib/k2i/txlog` | `transaction_log.log_dir` | Transaction log: offset markers, flush start/complete records, committed data-file records, idempotency records, checkpoints, schema-evolution and maintenance events. Newline-delimited JSON files named `txlog-<uuid>.jsonl`. | Required | `PersistentVolumeClaim`, `ReadWriteOnce` | K2I's recovery and idempotency history is gone. Kafka's group offsets are unaffected, so the consumer resumes from the last broker-committed offset; in-flight flush state, committed-file bookkeeping for the read RPC, and deduplication history cannot be reconstructed. Parquet files written by an incomplete flush are left as orphans. |
| `/var/lib/k2i/txlog/schema-cache` | none; derived from `transaction_log.log_dir` | Stale Schema Registry disk cache for Protobuf descriptors. | Optional | `emptyDir` | No data or offset loss. Descriptors are fetched again from Schema Registry. While the registry is unreachable, the stale-cache fallback is unavailable, so startup or schema resolution can fail if the registry is also down. |
| `/var/lib/k2i/warehouse` | `iceberg.warehouse_path` | **Local filesystem warehouses only.** Parquet data files plus Iceberg metadata and manifests. | Required only when `warehouse_path` is a local path | `PersistentVolumeClaim`, `ReadWriteOnce` | The cold copy of the data is gone; hot buffer contents are also lost on restart. If Kafka offsets were already committed, K2I does not replay the topic automatically, so recovery needs an operator reset of the consumer group. Catalog entries can point at files that no longer exist. |
| `/var/lib/k2i/catalog/catalog.db` | `iceberg.sql_catalog.url` | **`catalog_type = "sql"` with the SQLite backend only.** Namespace, table, current snapshot, and data-file rows. | Required for the SQLite SQL catalog | `PersistentVolumeClaim`, `ReadWriteOnce` | The catalog no longer knows the table or its files. Warehouse files remain on disk, but K2I cannot resolve them, and a later write can recreate the table empty. |

The warehouse row does **not** apply to object-store warehouses. With an
`iceberg.warehouse_path` of `s3://`, `gs://`, or `az://`/`abfs://`, Parquet and
Iceberg metadata live outside the pod; only the transaction log (and the SQL
catalog database, if used) needs local persistence.

The transaction log default is `./transaction_logs`, relative to the process
working directory. Set `transaction_log.log_dir` explicitly in containers so
the path does not depend on the image working directory.

If `rpc.enabled = true`, the read-state server writes a Unix socket at
`rpc.socket_path` (default `./run/k2i.sock`, relative to the working
directory). The socket is transient: mount a writable `emptyDir` and set an
absolute path under it when RPC is enabled; it needs no PVC.

Recovery limits — computed-but-unapplied offsets, per-entry fsync, and async
Kafka commits — are summarized in
[Persistence and recovery](./production-readiness.md#persistence-and-recovery).

### Sizing and access modes

- **Access mode:** use `ReadWriteOnce`. K2I is a single-process writer; never
  run two pods against the same transaction-log or warehouse volume. Set
  `strategy: Recreate` so a rollout stops the old pod before the new one
  starts.
- **Transaction log:** one `OffsetMarker` is written per message. The log
  rotates at `checkpoint_interval_entries` (default 10,000) or
  `checkpoint_interval_seconds` (default 300), keeping `max_log_files`
  (default 10) files. Each marker is a short JSON line of roughly 150–200
  bytes; size the volume from your message rate and start at 10Gi for
  low-to-moderate rates. Monitor the directory and raise it as throughput
  grows.
- **Local warehouse:** size to the retained Parquet bytes plus Iceberg metadata
  and manifests. Maintenance scheduling is not fully wired for every
  deployment, so plan for growth instead of assuming retention will free space.
- **SQLite catalog:** small — MiB for most tables, one row per data file. 1Gi
  is ample to start.
- **Schema cache:** a few MiB. An `emptyDir` with no size limit is enough; it
  counts against the node's ephemeral storage. Because the cache lives inside
  `transaction_log.log_dir`, the example below mounts an `emptyDir` over that
  subdirectory, leaving the rest of the transaction log on the PVC.

### Example: local warehouse on PVCs

The manifest below persists the transaction log, the local warehouse, and the
SQLite catalog, and keeps the schema cache ephemeral. It uses the embedded SQL
catalog (`catalog_type = "sql"`) so the example needs no external catalog
service; replace the ConfigMap with your own configuration. With an object-store
warehouse, drop the warehouse and catalog claims and keep the transaction-log
claim.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: k2i-config
data:
  config.toml: |
    [kafka]
    bootstrap_servers = ["kafka:9092"]
    topic = "events"
    consumer_group = "k2i-ingestion"

    [kafka.format]
    type = "raw"

    [iceberg]
    catalog_type = "sql"
    warehouse_path = "/var/lib/k2i/warehouse"
    database_name = "raw"
    table_name = "events"

    [iceberg.sql_catalog]
    type = "sqlite"
    url = "sqlite:///var/lib/k2i/catalog/catalog.db"
    catalog_name = "k2i"

    [transaction_log]
    log_dir = "/var/lib/k2i/txlog"
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: k2i-txlog
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: k2i-warehouse
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: k2i-catalog
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: k2i
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: k2i
  template:
    metadata:
      labels:
        app: k2i
    spec:
      securityContext:
        runAsUser: 1000
        runAsGroup: 1000
        fsGroup: 1000
      containers:
        - name: k2i
          image: ghcr.io/osodevops/k2i:latest
          args: ["ingest", "--config", "/etc/k2i/config.toml"]
          ports:
            - name: health
              containerPort: 8080
            - name: metrics
              containerPort: 9090
          volumeMounts:
            - name: config
              mountPath: /etc/k2i
              readOnly: true
            - name: txlog
              mountPath: /var/lib/k2i/txlog
            # Shadows only the cache subdirectory; the rest of the txlog stays
            # on the PVC.
            - name: schema-cache
              mountPath: /var/lib/k2i/txlog/schema-cache
            - name: warehouse
              mountPath: /var/lib/k2i/warehouse
            - name: catalog
              mountPath: /var/lib/k2i/catalog
          livenessProbe:
            httpGet:
              path: /healthz
              port: health
            initialDelaySeconds: 10
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /readyz
              port: health
            initialDelaySeconds: 5
            periodSeconds: 10
      volumes:
        - name: config
          configMap:
            name: k2i-config
        - name: txlog
          persistentVolumeClaim:
            claimName: k2i-txlog
        - name: schema-cache
          emptyDir: {}
        - name: warehouse
          persistentVolumeClaim:
            claimName: k2i-warehouse
        - name: catalog
          persistentVolumeClaim:
            claimName: k2i-catalog
```

## Pattern A: projected secret files

Mount a `Secret` as files and point secret fields at the mount paths.
Secret fields (`kafka.security.sasl_username`, `kafka.security.sasl_password`,
`iceberg.aws_access_key_id`, `iceberg.aws_secret_access_key`,
`iceberg.rest.credential`, `iceberg.rest.oauth2_client_id`,
`iceberg.rest.oauth2_client_secret`, `iceberg.azure_access_key`) accept either a plain string or a
`{ file = "path" }` table. File contents are trimmed; a missing file fails
startup with a clear error.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: k2i-secrets
stringData:
  kafka-password: hunter2
  aws-access-key-id: AKIA...
  aws-secret-access-key: ...
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: k2i
spec:
  replicas: 1 # K2I is single-process by design
  selector:
    matchLabels:
      app: k2i
  template:
    metadata:
      labels:
        app: k2i
    spec:
      containers:
        - name: k2i
          image: ghcr.io/osodevops/k2i:latest
          args: ["ingest", "--config", "/etc/k2i/config.toml"]
          volumeMounts:
            - name: config
              mountPath: /etc/k2i
            - name: secrets
              mountPath: /etc/secrets/k2i
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: k2i-config # non-sensitive TOML only
        - name: secrets
          secret:
            secretName: k2i-secrets
```

```toml
# /etc/k2i/config.toml (ConfigMap — no secrets here)
[kafka]
bootstrap_servers = ["kafka:9092"]
topic = "events"
consumer_group = "k2i-ingestion"

[kafka.security]
protocol = "SASL_SSL"
sasl_mechanism = "SCRAM-SHA-256"
sasl_password = { file = "/etc/secrets/k2i/kafka-password" }

[iceberg]
catalog_type = "rest"
rest_uri = "http://iceberg-rest:8181"
warehouse_path = "s3://lakehouse/warehouse"
database_name = "raw"
table_name = "events"
aws_access_key_id = { file = "/etc/secrets/k2i/aws-access-key-id" }
aws_secret_access_key = { file = "/etc/secrets/k2i/aws-secret-access-key" }
```

## Pattern B: environment variables

The fields listed in [Supported variables](#supported-variables) can be
overridden with a `K2I_` prefixed variable: `K2I_` + uppercase field path with
`_` separators. Secrets come from `secretKeyRef`; plain values from `env` or
`configMapKeyRef`.

Anything outside that table has no env override — set it in the TOML file. A
`K2I_*` variable that is not recognized is logged at startup, so a typo such as
`K2I_KAFKA_TOPC` is reported rather than silently ignored.

```yaml
spec:
  containers:
    - name: k2i
      env:
        - name: K2I_KAFKA_BOOTSTRAP_SERVERS
          value: "kafka:9092"
        - name: K2I_KAFKA_TOPIC
          value: "events"
        - name: K2I_KAFKA_SECURITY_SASL_PASSWORD
          valueFrom:
            secretKeyRef:
              name: k2i-secrets
              key: kafka-password
        - name: K2I_ICEBERG_AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: k2i-secrets
              key: aws-access-key-id
```

Note: `Vec<String>` fields such as `K2I_KAFKA_BOOTSTRAP_SERVERS` are
comma-separated.

### Supported variables

| Variable | Field |
|---|---|
| `K2I_KAFKA_BOOTSTRAP_SERVERS` | `kafka.bootstrap_servers` (comma-separated) |
| `K2I_KAFKA_TOPIC` | `kafka.topic` |
| `K2I_KAFKA_CONSUMER_GROUP` | `kafka.consumer_group` |
| `K2I_KAFKA_BATCH_SIZE` | `kafka.batch_size` |
| `K2I_KAFKA_BATCH_TIMEOUT_MS` | `kafka.batch_timeout_ms` |
| `K2I_KAFKA_SESSION_TIMEOUT_MS` | `kafka.session_timeout_ms` |
| `K2I_KAFKA_HEARTBEAT_INTERVAL_MS` | `kafka.heartbeat_interval_ms` |
| `K2I_KAFKA_MAX_POLL_INTERVAL_MS` | `kafka.max_poll_interval_ms` |
| `K2I_KAFKA_AUTO_OFFSET_RESET` | `kafka.auto_offset_reset` (`earliest`, `latest`) |
| `K2I_KAFKA_SECURITY_PROTOCOL` | `kafka.security.protocol` |
| `K2I_KAFKA_SECURITY_SASL_MECHANISM` | `kafka.security.sasl_mechanism` |
| `K2I_KAFKA_SECURITY_SASL_USERNAME` | `kafka.security.sasl_username` |
| `K2I_KAFKA_SECURITY_SASL_PASSWORD` | `kafka.security.sasl_password` |
| `K2I_ICEBERG_CATALOG_TYPE` | `iceberg.catalog_type` (`rest`, `glue`, `hive`, `nessie`, `sql`) |
| `K2I_ICEBERG_WAREHOUSE_PATH` | `iceberg.warehouse_path` |
| `K2I_ICEBERG_DATABASE_NAME` | `iceberg.database_name` |
| `K2I_ICEBERG_TABLE_NAME` | `iceberg.table_name` |
| `K2I_ICEBERG_AWS_REGION` | `iceberg.aws_region` |
| `K2I_ICEBERG_AWS_ACCESS_KEY_ID` | `iceberg.aws_access_key_id` |
| `K2I_ICEBERG_AWS_SECRET_ACCESS_KEY` | `iceberg.aws_secret_access_key` |
| `K2I_ICEBERG_GCS_BUCKET_NAME` | `iceberg.gcs_bucket_name` |
| `K2I_ICEBERG_GCS_SERVICE_ACCOUNT_PATH` | `iceberg.gcs_service_account_path` |
| `K2I_ICEBERG_AZURE_STORAGE_ACCOUNT_NAME` | `iceberg.azure_storage_account_name` (required for Azure) |
| `K2I_ICEBERG_AZURE_CONTAINER_NAME` | `iceberg.azure_container_name` |
| `K2I_ICEBERG_AZURE_ACCESS_KEY` | `iceberg.azure_access_key` |
| `K2I_ICEBERG_S3_ENDPOINT` | `iceberg.s3_endpoint` |
| `K2I_ICEBERG_REST_URI` | `iceberg.rest_uri` |
| `K2I_ICEBERG_HIVE_METASTORE_URI` | `iceberg.hive_metastore_uri` |
| `K2I_ICEBERG_REST_CREDENTIAL` | `iceberg.rest.credential` |
| `K2I_ICEBERG_REST_OAUTH2_CLIENT_ID` | `iceberg.rest.oauth2_client_id` |
| `K2I_ICEBERG_REST_OAUTH2_CLIENT_SECRET` | `iceberg.rest.oauth2_client_secret` |
| `K2I_SCHEMA_EVOLUTION_MODE` | `schema_evolution.mode` (`manual`, `auto-additive`, `permissive`) |
| `K2I_SCHEMA_EVOLUTION_ON_BREAKING_CHANGE` | `schema_evolution.on_breaking_change` (`pause`, `fail`, `skip-message`) |
| `K2I_BUFFER_TTL_SECONDS` | `buffer.ttl_seconds` |
| `K2I_BUFFER_MAX_SIZE_MB` | `buffer.max_size_mb` |
| `K2I_BUFFER_FLUSH_INTERVAL_SECONDS` | `buffer.flush_interval_seconds` |
| `K2I_TRANSACTION_LOG_LOG_DIR` | `transaction_log.log_dir` |
| `K2I_MONITORING_HEALTH_PORT` | `monitoring.health_port` |
| `K2I_MONITORING_METRICS_PORT` | `monitoring.metrics_port` |
| `K2I_MONITORING_LOG_LEVEL` | `monitoring.log_level` (`trace`, `debug`, `info`, `warn`, `error`) |
| `K2I_MONITORING_LOG_FORMAT` | `monitoring.log_format` (`json`, `text`) |
| `K2I_RPC_ENABLED` | `rpc.enabled` (`true`/`1`) |
| `K2I_RPC_SOCKET_PATH` | `rpc.socket_path` |

## Pattern C: Secrets Store CSI Driver

The CSI driver mounts external secrets (AWS Secrets Manager, Azure Key Vault,
GCP Secret Manager, Vault) as files. Use the same `{ file = ... }` TOML refs
as Pattern A.

```yaml
apiVersion: secrets-store.csi.x-k8s.io/v1
kind: SecretProviderClass
metadata:
  name: k2i-secrets
spec:
  provider: aws
  parameters:
    objects: |
      - objectName: "k2i/kafka-password"
        objectType: "secretsmanager"
      - objectName: "k2i/aws-secret-access-key"
        objectType: "secretsmanager"
---
# Pod spec:
      volumes:
        - name: secrets
          csi:
            driver: secrets-store.csi.k8s.io
            readOnly: true
            volumeAttributes:
              secretProviderClass: k2i-secrets
      containers:
        - name: k2i
          volumeMounts:
            - name: secrets
              mountPath: /mnt/secrets
              readOnly: true
```

```toml
[kafka.security]
sasl_password = { file = "/mnt/secrets/kafka-password" }

[iceberg]
aws_secret_access_key = { file = "/mnt/secrets/aws-secret-access-key" }
```

## Notes

- **Secret redaction**: secret fields are wrapped in a `Secret` type that
  redacts in **both** `Debug` and `serde` serialization, emitting `REDACTED` in
  place of the value. Neither a `{:?}` dump nor a JSON/TOML serialization of the
  configuration can leak a credential. The value is reachable only through the
  explicit `expose()` accessor. The trade-off is deliberate: a serialized
  configuration does not round-trip, because reading it back yields the literal
  `REDACTED` marker rather than the original credential.
- **Fail-fast validation**: warehouse settings that cannot be derived from the
  path — notably `azure_storage_account_name` — are checked at startup rather
  than on the first flush, so a misconfigured deployment fails immediately
  instead of after the process has reported healthy.
- **Env var visibility**: values injected via `env` are visible in
  `/proc/<pid>/environ` to other processes with sufficient privileges and in
  `kubectl describe pod` output is limited to the reference (not the value),
  but the pod spec still records the mapping. Prefer file refs for the most
  sensitive credentials.
- **Rotation**: file contents are read once at startup. Rotating a secret
  requires a pod restart to pick up new values.
- **Single replica**: K2I is single-process by design; run `replicas: 1`.
