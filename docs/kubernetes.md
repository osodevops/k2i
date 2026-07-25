# Deploying K2I on Kubernetes

K2I reads configuration from a TOML file and layers two secret-injection
mechanisms on top, so no secret ever needs to live in a ConfigMap:

1. **Secret file refs** — `{ file = "path" }` inline tables in TOML, designed
   for Kubernetes projected volumes and the Secrets Store CSI Driver.
2. **`K2I_*` environment variables** — override any field at runtime, designed
   for `env` / `envFrom` injection from `Secret` resources.

Precedence (highest wins):

1. `K2I_*` environment variables
2. Inline TOML values (including `{ file = ... }` refs)

Invalid numeric/enum env values are rejected with a warning (the TOML or
default value is preserved), and unrecognized `K2I_*` variables are logged so
typos do not fail silently.

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

Every field can be overridden with a `K2I_` prefixed variable:
`K2I_` + uppercase field path with `_` separators. Secrets come from
`secretKeyRef`; plain values from `env` or `configMapKeyRef`.

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

- **Secret redaction**: secret fields are wrapped in a `Secret` type whose
  `Debug` output is `Secret(REDACTED)`, so `{:?}` dumps of the configuration
  do not leak values. The value is only exposed through explicit accessors.
- **Env var visibility**: values injected via `env` are visible in
  `/proc/<pid>/environ` to other processes with sufficient privileges and in
  `kubectl describe pod` output is limited to the reference (not the value),
  but the pod spec still records the mapping. Prefer file refs for the most
  sensitive credentials.
- **Rotation**: file contents are read once at startup. Rotating a secret
  requires a pod restart to pick up new values.
- **Single replica**: K2I is single-process by design; run `replicas: 1`.
