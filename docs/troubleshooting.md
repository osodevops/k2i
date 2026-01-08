# K2I Troubleshooting Guide

This guide covers common issues and their solutions when running K2I.

## Diagnostic Commands

### Check Health Status

```bash
# Via CLI
k2i status --url http://localhost:8080

# Via curl
curl -s http://localhost:8080/health | jq .

# Kubernetes
kubectl exec -it deployment/k2i -n k2i -- curl -s localhost:8080/health
```

### Check Logs

```bash
# Systemd
journalctl -u k2i -f

# Docker
docker logs -f k2i

# Kubernetes
kubectl logs -f deployment/k2i -n k2i
```

### Enable Debug Logging

```bash
# Via CLI flag
k2i ingest -v      # debug level
k2i ingest -vv     # trace level

# Via environment variable
RUST_LOG=k2i=debug k2i ingest
RUST_LOG=k2i=trace,rdkafka=debug k2i ingest
```

### Check Prometheus Metrics

```bash
curl -s http://localhost:9090/metrics | grep k2i
```

## Common Issues

### Startup Issues

#### Config File Not Found

**Symptom:**
```
Error: No such file or directory (os error 2)
```

**Cause:** Configuration file doesn't exist at the specified path.

**Solution:**
```bash
# Check file exists
ls -la config.toml

# Specify full path
k2i ingest --config /absolute/path/to/config.toml
```

#### Invalid TOML Syntax

**Symptom:**
```
Error: TOML parse error at line 15, column 3
```

**Cause:** Syntax error in configuration file.

**Solution:**
```bash
# Validate config
k2i validate --config config.toml

# Check TOML syntax
cat config.toml | toml-cli lint
```

#### Missing Required Fields

**Symptom:**
```
Error: missing field `topic` at line 1 column 1
```

**Cause:** Required configuration field is not set.

**Solution:** Add the missing field to your config file. Required fields:
- `kafka.bootstrap_servers`
- `kafka.topic`
- `kafka.consumer_group`
- `iceberg.catalog_type`
- `iceberg.warehouse_path`
- `iceberg.database_name`
- `iceberg.table_name`

### Kafka Connection Issues

#### Cannot Connect to Brokers

**Symptom:**
```
Error: Kafka connection failed: broker not available
```

**Causes:**
1. Brokers are not running
2. Network connectivity issue
3. Wrong bootstrap servers

**Solutions:**
```bash
# Test connectivity
nc -zv kafka-broker 9092

# Check broker status
kafka-broker-api-versions --bootstrap-server localhost:9092

# Verify config
cat config.toml | grep bootstrap_servers
```

#### SASL Authentication Failed

**Symptom:**
```
Error: SASL authentication failed: Invalid credentials
```

**Causes:**
1. Wrong username/password
2. Wrong SASL mechanism
3. Missing security configuration

**Solution:**
```toml
[kafka.security]
protocol = "SASL_SSL"
sasl_mechanism = "SCRAM-SHA-256"  # or SCRAM-SHA-512, PLAIN
sasl_username = "correct-username"
sasl_password = "correct-password"
```

#### SSL Certificate Errors

**Symptom:**
```
Error: SSL handshake failed: certificate verify failed
```

**Solutions:**
```toml
[kafka.security]
protocol = "SASL_SSL"
ssl_ca_location = "/path/to/ca-cert.pem"

# For self-signed certs (development only)
# ssl_endpoint_identification_algorithm = ""
```

#### Consumer Group Rebalance Loops

**Symptom:**
```
WARN Consumer rebalance triggered
WARN Consumer rebalance triggered
WARN Consumer rebalance triggered
```

**Cause:** `max_poll_interval_ms` too short for flush time.

**Solution:**
```toml
[kafka]
max_poll_interval_ms = 600000  # 10 minutes
```

### Buffer Issues

#### Buffer Full / Backpressure

**Symptom:**
```
WARN Backpressure engaged: buffer full
```

**Causes:**
1. Iceberg writes too slow
2. Buffer too small for throughput
3. Network issues to object storage

**Solutions:**
```toml
[buffer]
# Increase buffer size
max_size_mb = 1000

# Decrease flush interval (write more often)
flush_interval_seconds = 15

# Decrease batch size (smaller files, faster writes)
flush_batch_size = 5000
```

#### Out of Memory

**Symptom:**
```
Error: memory allocation failed
```

**Causes:**
1. Buffer size exceeds available memory
2. Memory leak (report as bug)

**Solutions:**
```toml
[buffer]
# Reduce buffer size to fit in available memory
max_size_mb = 500
```

```bash
# Increase container/process memory limit
docker run -m 4g ...

# Kubernetes
resources:
  limits:
    memory: 4Gi
```

### Iceberg Issues

#### Catalog Connection Failed

**Symptom:**
```
Error: Failed to connect to Iceberg catalog
```

**Causes:**
1. Catalog server not running
2. Wrong URI
3. Network issue

**Solutions:**
```bash
# Test REST catalog
curl -s http://localhost:8181/v1/config

# Check Glue access
aws glue get-databases --region us-east-1

# Verify config
cat config.toml | grep -A5 "\[iceberg\]"
```

#### Table Not Found

**Symptom:**
```
Error: Table not found: analytics.events
```

**Cause:** Table doesn't exist in catalog.

**Solution:**
```bash
# Create table via Spark/DuckDB/Trino
spark-sql -e "CREATE TABLE analytics.events ..."

# Or use catalog REST API
curl -X POST http://localhost:8181/v1/namespaces/analytics/tables ...
```

#### CAS Conflict (Concurrent Writes)

**Symptom:**
```
Error: CAS conflict: expected snapshot 123, actual 124
```

**Cause:** Another process modified the table.

**Solution:** K2I automatically retries CAS conflicts. If persistent:
1. Ensure only one K2I instance per table
2. Check for other Iceberg writers

#### S3 Permission Denied

**Symptom:**
```
Error: Access Denied (Service: S3)
```

**Solutions:**
```bash
# Test S3 access
aws s3 ls s3://your-bucket/warehouse/

# Check IAM permissions
aws sts get-caller-identity

# Verify credentials in config
cat config.toml | grep aws_
```

Required S3 permissions:
- `s3:GetObject`
- `s3:PutObject`
- `s3:DeleteObject`
- `s3:ListBucket`

#### Parquet Write Errors

**Symptom:**
```
Error: Failed to write Parquet file
```

**Causes:**
1. Schema mismatch
2. Invalid data types
3. Storage full

**Solutions:**
```bash
# Check available storage
aws s3api head-bucket --bucket your-bucket

# Enable trace logging for details
RUST_LOG=k2i::iceberg=trace k2i ingest
```

### Transaction Log Issues

#### Log Corruption

**Symptom:**
```
Error: Transaction log corrupted at position 12345
```

**Cause:** Incomplete write, disk failure, or bug.

**Solutions:**
```bash
# Backup corrupted log
cp -r transaction_logs transaction_logs.backup

# Clear log (will replay from Kafka)
rm -rf transaction_logs/*

# Restart (will re-consume from last Kafka offset)
k2i ingest
```

#### Checksum Mismatch

**Symptom:**
```
Error: Checksum mismatch at entry 5678
```

**Cause:** Data corruption in log file.

**Solution:** Same as log corruption - backup and clear.

#### Recovery Failed

**Symptom:**
```
Error: Failed to recover from transaction log
```

**Solutions:**
1. Check disk space
2. Check file permissions
3. Clear transaction log if corrupt
4. Check Iceberg table state matches expected

### Performance Issues

#### High Consumer Lag

**Symptom:** `k2i_kafka_consumer_lag` increasing over time.

**Causes:**
1. Processing too slow
2. Flush taking too long
3. Insufficient resources

**Solutions:**
```toml
[kafka]
# Increase batch size for higher throughput
batch_size = 5000

[buffer]
# Larger flushes are more efficient
flush_batch_size = 50000

[iceberg]
# Faster compression
compression = "lz4"  # instead of zstd
```

#### Slow Flush Times

**Symptom:** `k2i_flush_duration_seconds` high (> 5s).

**Causes:**
1. Large batch sizes
2. Slow object storage
3. Network latency

**Solutions:**
```toml
[buffer]
# Smaller, more frequent flushes
flush_batch_size = 10000
flush_interval_seconds = 15

[iceberg]
# Faster compression
compression = "snappy"  # fastest

# Smaller files
target_file_size_mb = 128
```

#### High Memory Usage

**Symptom:** Container/process memory growing.

**Solutions:**
```toml
[buffer]
# Limit buffer size
max_size_mb = 500

# Shorter TTL evicts faster
ttl_seconds = 30
```

### Health Check Issues

#### Degraded Status

**Symptom:**
```json
{"status": "Degraded", "components": {"catalog": {"status": "Degraded"}}}
```

**Cause:** Non-critical component experiencing issues but still operational.

**Action:** Check component logs for warnings, monitor closely.

#### Unhealthy Status

**Symptom:**
```json
{"status": "Unhealthy", "components": {"kafka": {"status": "Unhealthy"}}}
```

**Cause:** Critical component failed.

**Action:** Check component-specific errors, restart may be needed.

#### Health Endpoint Not Responding

**Causes:**
1. K2I not running
2. Wrong port
3. Network issue

**Solutions:**
```bash
# Check process is running
ps aux | grep k2i
pgrep -f "k2i ingest"

# Check port binding
netstat -tlnp | grep 8080
ss -tlnp | grep 8080

# Check firewall
iptables -L -n | grep 8080
```

## Exit Code Reference

| Code | Meaning | Action |
|------|---------|--------|
| 0 | Success | None |
| 1 | Config Error | Fix configuration |
| 2 | Kafka Error | Check Kafka connectivity |
| 3 | Iceberg Error | Check catalog/storage |
| 4 | Storage Error | Check object storage access |
| 5 | TxLog Error | Check local disk |
| 6 | Health Error | Check all components |
| 10 | Runtime Error | Check logs for details |
| 130 | Signal Interrupt | Normal shutdown |

## Getting Help

### Collecting Debug Information

When reporting issues, include:

1. **Version:**
   ```bash
   k2i --version
   ```

2. **Configuration (redact secrets):**
   ```bash
   cat config.toml | sed 's/password.*/password = "REDACTED"/g'
   ```

3. **Health status:**
   ```bash
   curl -s http://localhost:8080/health | jq .
   ```

4. **Metrics:**
   ```bash
   curl -s http://localhost:9090/metrics | grep k2i
   ```

5. **Logs (last 100 lines with trace):**
   ```bash
   RUST_LOG=trace k2i ingest 2>&1 | tail -100
   ```

6. **Environment:**
   ```bash
   uname -a
   rustc --version
   docker --version  # if applicable
   kubectl version   # if applicable
   ```

### Reporting Issues

Report issues at: https://github.com/osodevops/k2i/issues

Include:
- Description of the problem
- Steps to reproduce
- Expected vs actual behavior
- Debug information (see above)

## FAQ

### Can I run multiple K2I instances for the same topic?

No. K2I uses a single consumer group per topic. Multiple instances would cause duplicate processing. Run one instance per topic.

### How do I recover from a crash?

K2I automatically recovers on restart using the transaction log. Simply restart the process - it will resume from the last committed offset.

### How do I change the Kafka topic without losing data?

1. Stop K2I gracefully (flushes buffer)
2. Update configuration
3. Start K2I - it will start from the new topic's beginning

### How do I migrate to a new Iceberg table?

1. Stop K2I
2. Update `database_name` and `table_name` in config
3. Create the new table in your catalog
4. Start K2I

### What happens if the buffer fills up?

K2I engages backpressure - it pauses Kafka consumption until the buffer has space. Data is not lost, but latency increases.

### How do I scale K2I?

K2I is single-process by design. Scale by:
1. Running separate instances for different topics
2. Partitioning your data across multiple topics
3. Increasing resources (CPU, memory) for a single instance
