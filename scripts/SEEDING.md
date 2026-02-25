# OTLP Ingestion Walkthrough (Standalone Script)

This guide explains how to use the **Enhanced Seeding Script** to ingest a high volume of diverse OTLP telemetry for testing and developmental purposes.

## Using the Seeding Script

The script `scripts/seed_single_user_metrics.py` handles the entire ingestion process in one go.

### 1. Requirements
None! The script uses standard Python libraries and the backend APIs.

### 2. Run the Script
Run the script from the **project root** directory using a new email address to see the results. It will automatically:
- Sign up a new user and team.
- Capture the API key.
- Generate 100+ records for **Gauges**, **Sums**, **Histograms**, **Logs**, and **Traces**.

```bash
python3 scripts/seed_single_user_metrics.py --email your-email@example.com --samples 20
```

### Telemetry Diversity

#### 📈 Metrics
- **Gauges**: Infrastructure metrics (System CPU/Memory, Kafka lag, Redis usage).
- **Sums (Counters)**: Monotonically increasing request counts (`http.server.requests.count`).
- **Histograms**: Request duration with explicit bounds and statistical summaries.

#### 📜 Logs
Generates logs for 7 distinct categories with OTLP-style attributes:
- `kafka`, `db`, `system`, `http`, `rpc`, `auth`, `worker`

#### 🔗 Traces
Generates 100+ spans with parent-child relationships, simulating a distributed request flow across multiple services.

## Verification
After the script completes, it will output the total counts of ingested telemetry. You can then log in to the dashboard using the credentials provided in the script's output to view the data.
