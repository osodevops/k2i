# K2I Deployment Guide

This guide covers production deployment strategies for K2I.

## Deployment Options

| Option | Use Case | Complexity |
|--------|----------|------------|
| Binary | Single server, VMs | Low |
| Docker | Containers, local dev | Low |
| Kubernetes | Production, scaling | Medium |
| Systemd | Linux services | Low |

## Building for Production

### Optimized Release Build

```bash
# Standard release build
cargo build --release

# With Link-Time Optimization (smaller, faster)
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### Static Binary (musl)

For portable Linux binaries:

```bash
# Add musl target
rustup target add x86_64-unknown-linux-musl

# Build static binary
cargo build --release --target x86_64-unknown-linux-musl
```

### Cross-Compilation

```bash
# macOS to Linux
cargo build --release --target x86_64-unknown-linux-gnu

# Linux to macOS (requires osxcross)
cargo build --release --target x86_64-apple-darwin
```

## Docker Deployment

### Dockerfile

```dockerfile
# Build stage
FROM rust:1.75-bookworm AS builder

WORKDIR /build

# Install dependencies for rdkafka
RUN apt-get update && apt-get install -y \
    cmake \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy source
COPY . .

# Build release binary
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/k2i /usr/local/bin/k2i

# Create non-root user
RUN useradd -r -u 1000 k2i
USER k2i

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/healthz || exit 1

EXPOSE 8080 9090

ENTRYPOINT ["k2i"]
CMD ["ingest", "--config", "/etc/k2i/config.toml"]
```

### Docker Compose

```yaml
version: '3.8'

services:
  k2i:
    build: .
    image: k2i:latest
    container_name: k2i
    ports:
      - "8080:8080"   # Health
      - "9090:9090"   # Metrics
    volumes:
      - ./config.toml:/etc/k2i/config.toml:ro
      - k2i-txlog:/var/lib/k2i/txlog
    environment:
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      - KAFKA_PASSWORD=${KAFKA_PASSWORD}
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/healthz"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 10s

volumes:
  k2i-txlog:
```

### Running with Docker

```bash
# Build image
docker build -t k2i:latest .

# Run container
docker run -d \
  --name k2i \
  -p 8080:8080 \
  -p 9090:9090 \
  -v $(pwd)/config.toml:/etc/k2i/config.toml:ro \
  -v k2i-txlog:/var/lib/k2i/txlog \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  k2i:latest

# View logs
docker logs -f k2i

# Stop gracefully
docker stop k2i
```

## Kubernetes Deployment

### Namespace

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: k2i
```

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: k2i-config
  namespace: k2i
data:
  config.toml: |
    [kafka]
    bootstrap_servers = ["kafka.kafka.svc.cluster.local:9092"]
    topic = "events"
    consumer_group = "k2i-production"
    batch_size = 5000

    [iceberg]
    catalog_type = "rest"
    rest_uri = "http://iceberg-rest.iceberg.svc.cluster.local:8181"
    warehouse_path = "s3://data-lake/iceberg/"
    database_name = "analytics"
    table_name = "events"
    compression = "zstd"

    [buffer]
    ttl_seconds = 120
    max_size_mb = 1000
    flush_interval_seconds = 60
    flush_batch_size = 50000

    [transaction_log]
    log_dir = "/var/lib/k2i/txlog"

    [maintenance]
    compaction_enabled = true
    compaction_interval_seconds = 7200

    [monitoring]
    metrics_port = 9090
    health_port = 8080
    log_level = "info"
    log_format = "json"
```

### Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: k2i-secrets
  namespace: k2i
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: "AKIAIOSFODNN7EXAMPLE"
  AWS_SECRET_ACCESS_KEY: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  KAFKA_PASSWORD: "your-kafka-password"
```

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: k2i
  namespace: k2i
  labels:
    app: k2i
spec:
  replicas: 1  # Single instance per topic
  selector:
    matchLabels:
      app: k2i
  template:
    metadata:
      labels:
        app: k2i
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: k2i
      securityContext:
        runAsUser: 1000
        runAsGroup: 1000
        fsGroup: 1000
      containers:
        - name: k2i
          image: ghcr.io/osodevops/k2i:latest
          args:
            - ingest
            - --config
            - /etc/k2i/config.toml
          ports:
            - name: health
              containerPort: 8080
            - name: metrics
              containerPort: 9090
          envFrom:
            - secretRef:
                name: k2i-secrets
          volumeMounts:
            - name: config
              mountPath: /etc/k2i
              readOnly: true
            - name: txlog
              mountPath: /var/lib/k2i/txlog
          resources:
            requests:
              memory: "2Gi"
              cpu: "1000m"
            limits:
              memory: "4Gi"
              cpu: "2000m"
          livenessProbe:
            httpGet:
              path: /healthz
              port: health
            initialDelaySeconds: 10
            periodSeconds: 30
            timeoutSeconds: 5
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /readyz
              port: health
            initialDelaySeconds: 5
            periodSeconds: 10
            timeoutSeconds: 5
            failureThreshold: 3
      volumes:
        - name: config
          configMap:
            name: k2i-config
        - name: txlog
          persistentVolumeClaim:
            claimName: k2i-txlog

  strategy:
    type: Recreate  # Important: only one instance should run
```

### PersistentVolumeClaim

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: k2i-txlog
  namespace: k2i
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
  storageClassName: gp3
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: k2i
  namespace: k2i
  labels:
    app: k2i
spec:
  type: ClusterIP
  ports:
    - name: health
      port: 8080
      targetPort: 8080
    - name: metrics
      port: 9090
      targetPort: 9090
  selector:
    app: k2i
```

### ServiceMonitor (Prometheus Operator)

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: k2i
  namespace: k2i
  labels:
    app: k2i
spec:
  selector:
    matchLabels:
      app: k2i
  endpoints:
    - port: metrics
      interval: 30s
      path: /metrics
```

### RBAC (if using AWS IRSA)

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: k2i
  namespace: k2i
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/k2i-role
```

## Systemd Deployment

### Service File

Create `/etc/systemd/system/k2i.service`:

```ini
[Unit]
Description=K2I Kafka to Iceberg Ingestion
Documentation=https://github.com/osodevops/k2i
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=k2i
Group=k2i
ExecStart=/usr/local/bin/k2i ingest --config /etc/k2i/config.toml
ExecReload=/bin/kill -HUP $MAINPID
Restart=always
RestartSec=10
TimeoutStopSec=60

# Security
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=/var/lib/k2i

# Environment
EnvironmentFile=-/etc/k2i/environment
Environment=RUST_LOG=info

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096
MemoryMax=4G

[Install]
WantedBy=multi-user.target
```

### Environment File

Create `/etc/k2i/environment`:

```bash
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
KAFKA_PASSWORD=your-kafka-password
```

### Installation

```bash
# Create user
sudo useradd -r -s /bin/false k2i

# Create directories
sudo mkdir -p /etc/k2i /var/lib/k2i/txlog
sudo chown k2i:k2i /var/lib/k2i/txlog

# Copy binary
sudo cp target/release/k2i /usr/local/bin/
sudo chmod +x /usr/local/bin/k2i

# Copy config
sudo cp config.toml /etc/k2i/
sudo chmod 640 /etc/k2i/config.toml

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable k2i
sudo systemctl start k2i

# Check status
sudo systemctl status k2i
sudo journalctl -u k2i -f
```

## High Availability Considerations

### Single Instance per Topic

K2I is designed as a single-process application per Kafka topic. For high availability:

1. **Active-Passive**: Run standby instances that take over on failure
2. **Multiple Topics**: Run separate K2I instances for different topics
3. **Kubernetes**: Use Deployment with `replicas: 1` and `Recreate` strategy

### Graceful Failover

```yaml
# Kubernetes: Pod Disruption Budget
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: k2i
  namespace: k2i
spec:
  maxUnavailable: 0
  selector:
    matchLabels:
      app: k2i
```

### Transaction Log Persistence

The transaction log must persist across restarts for exactly-once semantics:

- **Kubernetes**: Use PersistentVolumeClaim
- **Docker**: Use named volumes
- **Bare metal**: Use reliable local storage or NFS

## Monitoring Setup

### Prometheus Configuration

```yaml
scrape_configs:
  - job_name: 'k2i'
    static_configs:
      - targets: ['k2i:9090']
    metrics_path: /metrics
    scrape_interval: 30s
```

### Grafana Dashboard

Key panels to include:

1. **Throughput**: `rate(k2i_messages_total[5m])`
2. **Buffer Size**: `k2i_buffer_size_bytes`
3. **Flush Duration**: `histogram_quantile(0.99, k2i_flush_duration_seconds_bucket)`
4. **Error Rate**: `rate(k2i_errors_total[5m])`
5. **Consumer Lag**: `k2i_kafka_consumer_lag`

### Alerting Rules

```yaml
groups:
  - name: k2i
    rules:
      - alert: K2IUnhealthy
        expr: up{job="k2i"} == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: K2I instance is down

      - alert: K2IHighErrorRate
        expr: rate(k2i_errors_total[5m]) > 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: K2I error rate is elevated

      - alert: K2IBufferNearFull
        expr: k2i_buffer_size_bytes / (500 * 1024 * 1024) > 0.9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: K2I buffer is near capacity

      - alert: K2IHighConsumerLag
        expr: k2i_kafka_consumer_lag > 100000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: K2I consumer lag is high
```

## Security Best Practices

### Network Security

```yaml
# Kubernetes NetworkPolicy
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: k2i
  namespace: k2i
spec:
  podSelector:
    matchLabels:
      app: k2i
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: monitoring
      ports:
        - port: 8080
        - port: 9090
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: kafka
      ports:
        - port: 9092
    - to:
        - namespaceSelector:
            matchLabels:
              name: iceberg
      ports:
        - port: 8181
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0  # For S3
      ports:
        - port: 443
```

### Secrets Management

- Use Kubernetes Secrets or external secret managers (Vault, AWS Secrets Manager)
- Never commit secrets to version control
- Rotate credentials regularly
- Use IAM roles for AWS instead of access keys when possible

### Least Privilege

AWS IAM policy for K2I:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::data-lake",
        "arn:aws:s3:::data-lake/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:UpdateTable",
        "glue:CreateTable"
      ],
      "Resource": [
        "arn:aws:glue:*:*:catalog",
        "arn:aws:glue:*:*:database/analytics",
        "arn:aws:glue:*:*:table/analytics/*"
      ]
    }
  ]
}
```

## Resource Sizing

### Memory Requirements

| Workload | Buffer Size | Total Memory |
|----------|-------------|--------------|
| Light (< 1K msg/s) | 100 MB | 512 MB |
| Medium (1-10K msg/s) | 500 MB | 2 GB |
| Heavy (10-100K msg/s) | 2 GB | 4 GB |
| Very Heavy (> 100K msg/s) | 4 GB | 8 GB |

### CPU Requirements

| Workload | CPU Cores |
|----------|-----------|
| Light | 0.5 |
| Medium | 1 |
| Heavy | 2 |
| Very Heavy | 4 |

### Storage Requirements

Transaction log size: ~1 GB per million messages processed

Iceberg table size: Depends on data volume and retention

## Upgrade Procedure

### Rolling Update (Kubernetes)

```bash
# Update image
kubectl set image deployment/k2i k2i=ghcr.io/osodevops/k2i:v0.2.0 -n k2i

# Monitor rollout
kubectl rollout status deployment/k2i -n k2i

# Rollback if needed
kubectl rollout undo deployment/k2i -n k2i
```

### Manual Upgrade

1. Stop K2I gracefully (waits for buffer flush)
2. Backup transaction log directory
3. Replace binary
4. Start K2I
5. Verify health and metrics

```bash
# Stop
sudo systemctl stop k2i

# Backup
sudo cp -r /var/lib/k2i/txlog /var/lib/k2i/txlog.backup

# Upgrade
sudo cp k2i-new /usr/local/bin/k2i

# Start
sudo systemctl start k2i

# Verify
k2i status
```
