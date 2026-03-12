# Log Ingestion Scripts

This directory contains bash scripts for ingesting various types of logs into the observability platform.

## `ingest_logs_hour.sh`

Generates realistic logs over approximately 1 hour with multiple services and log levels.

### Features

- **Multiple Services**: orders-service, payments-service, inventory-service, auth-service, cache-service, database-service
- **Log Levels**: DEBUG (50%), INFO (25%), WARN (15%), ERROR (10%)
- **Realistic Scenarios**:
  - Order creation and payment processing
  - Database queries and connection management
  - External API calls and latency issues
  - Cache misses and memory pressure
  - Timeouts, SQL exceptions, and connection pool exhaustion
- **Trace Correlation**: Each log includes random trace IDs and span IDs for correlation
- **Batch Sending**: Groups related logs and sends them via OTLP gRPC every 5 seconds

### Prerequisites

Ensure these tools are installed:
```bash
brew install grpcurl openssl  # macOS
# or on Linux:
apt-get install grpcurl openssl
```

### Configuration

Set these environment variables before running:

```bash
export OTLP_GRPC="localhost:4317"           # OTLP gRPC endpoint
export OTLP_API_KEY="your-api-key"         # API key for authentication
export API_BASE="http://localhost:9090"    # (optional) for dashboard verification
```

### Usage

Run the script:
```bash
./ingest_logs_hour.sh
```

Expected output:
```
Starting log ingestion loop...
Target: localhost:4317
API Key: your-api-key
Duration: ~1 hour (logs every 5 seconds)

[##################################################] 100% (720/720 iterations)

✓ Log ingestion complete!
  Generated logs from 720 iterations across 6 services
  Approximate duration: 1 hour
```

### What Gets Generated

Over the 1-hour period, you'll get:
- **360 DEBUG logs** (50% of batches)
- **180 INFO logs** (25% of batches)
- **108 WARN logs** (15% of batches)
- **72 ERROR logs** (10% of batches)

Total: ~1,800+ individual log records spread across 6 services.

### Sample Log Scenarios

**DEBUG** — Low-level execution details:
```
Executing database query for service=orders-service, execution_plan=optimized
```

**INFO** — Normal operations:
```
Order #15042 created successfully
Payment processed for customer 7834
Inventory updated: sku-4521, qty=42
```

**WARN** — Anomalies that don't block operation:
```
High latency detected on inventory-service call: 450ms
Cache miss rate exceeds threshold: 35%
Memory usage approaching limit: 85% of heap
```

**ERROR** — Critical failures:
```
Connection timeout to external API
Database connection pool exhausted
```

### Verifying Logs in Dashboard

Once ingestion completes, verify logs using the dashboard APIs:

```bash
# Logs histogram (count over time)
curl -s -G \
  -H "Authorization: Bearer ${JWT_TOKEN}" \
  --data-urlencode "start=$(($(date +%s) * 1000 - 3600000))" \
  --data-urlencode "end=$(($(date +%s) * 1000))" \
  --data-urlencode "services=orders-service" \
  --data-urlencode "step=1m" \
  "http://localhost:9090/api/v1/logs/histogram"

# Logs search with filter
curl -s -G \
  -H "Authorization: Bearer ${JWT_TOKEN}" \
  --data-urlencode "search=ERROR" \
  --data-urlencode "limit=50" \
  "http://localhost:9090/api/v1/logs"

# Logs statistics
curl -s -G \
  -H "Authorization: Bearer ${JWT_TOKEN}" \
  --data-urlencode "services=orders-service" \
  "http://localhost:9090/api/v1/logs/stats"
```

### How It Works

1. **Every 5 seconds**, generates a batch of 1-3 logs
2. **Randomly picks** a service from the 6 available
3. **Generates random trace/span IDs** for correlation
4. **Creates logs** with appropriate severity, attributes, and messages
5. **Sends via gRPC** to the OTLP endpoint with x-api-key authentication
6. **Repeats** for 720 iterations (~1 hour total)

### Customization

To modify the script:

- **Change services**: Edit the `SERVICES=()` array
- **Adjust log distribution**: Modify the percentages in the `case` statement
- **Duration**: Change `max_iterations=720` (each iteration = 5 seconds)
- **Log messages**: Update the message strings in the generator functions
- **Attributes**: Add/remove attributes in `generate_*_log()` functions

### Troubleshooting

**Error: grpcurl not found**
```bash
brew install grpcurl  # macOS
apt-get install grpcurl  # Linux
```

**Error: Connection refused**
- Ensure the OTLP gRPC server is running on the configured endpoint
- Check `OTLP_GRPC` environment variable

**Error: Authentication failed**
- Verify `OTLP_API_KEY` is correct
- Check server logs for auth errors

**Logs not appearing in dashboard**
- Wait a few seconds for ClickHouse to process
- Verify logs are being sent (check for errors in script output)
- Check dashboard API response for data presence
