# Load Testing Guide

## Overview

Self-contained load testing infrastructure that provisions a **dedicated Multipass VM** for isolated, reproducible benchmarking. The VM runs the full observability stack via Docker Compose, ingests ~50K records of metrics/logs/traces, benchmarks core query APIs, and produces a performance report.

**Why a VM?** Running containers directly on your host machine produces noisy results — your IDE, browser, OS processes, and other apps compete for CPU/memory/IO, making benchmarks unreproducible. The VM provides a fixed, isolated environment with dedicated resources.

## Prerequisites

- **Multipass** — lightweight Ubuntu VM manager
  - macOS: `brew install multipass`
  - Linux: `sudo snap install multipass`
- Python 3.8+ (on host, only for viewing reports)
- At least **6GB free RAM** (4GB for VM + headroom for host)

## Quick Start

```bash
cd loadtest
chmod +x run.sh
./run.sh
```

This single command will:
1. Launch a Multipass VM (`obs-loadtest`, 2 CPUs, 4GB RAM, 20GB disk)
2. Install Docker + Python inside the VM via cloud-init
3. Copy the project into the VM
4. Build the backend Docker image inside the VM
5. Start MariaDB, ClickHouse, Kafka, backend (x2), and nginx inside the VM
6. Run the full load test (ingestion + queries)
7. Copy the report back to your host
8. Ask whether to delete or keep the VM

## Architecture

```
Host machine (your laptop)
│
└── Multipass VM (obs-loadtest)
    │   2 CPUs, 4GB RAM — isolated, dedicated resources
    │
    ├── Docker bridge network
    │   ├── nginx (LB)        ──► backend:1, backend:2
    │   ├── mariadb:3306
    │   ├── clickhouse:9000
    │   └── kafka:9092
    │
    └── Python load_test.py (runs inside VM)
        ├── Phase 1: OTLP ingestion → backend → Kafka → ClickHouse
        └── Phase 2: Query APIs → backend → ClickHouse
```

Everything runs inside the VM. The load test client and servers share the VM's network (localhost), so network latency measurements reflect actual backend performance, not host-to-VM overhead.

## Customizing the Environment

Edit `.env` to change VM and infrastructure parameters:

```bash
# VM resources (increase for heavier tests)
VM_NAME=obs-loadtest
VM_CPUS=4
VM_MEMORY=8G
VM_DISK=40G

# Scale backend replicas (the "x" pods)
BACKEND_REPLICAS=4

# Scale ClickHouse (the "y" pods with "a" CPUs and "b" memory)
CLICKHOUSE_REPLICAS=1
CLICKHOUSE_CPUS=2.0
CLICKHOUSE_MEMORY=2g

# Tune Kafka consumer batching
QUEUE_BATCH_SIZE=1000
QUEUE_FLUSH_INTERVAL_MS=1000
```

### Available Parameters

| Variable | Default | Description |
|----------|---------|-------------|
| `VM_NAME` | obs-loadtest | Multipass VM name |
| `VM_CPUS` | 2 | CPUs allocated to the VM |
| `VM_MEMORY` | 4G | RAM allocated to the VM |
| `VM_DISK` | 20G | Disk allocated to the VM |
| `BACKEND_REPLICAS` | 2 | Number of backend instances behind nginx |
| `CLICKHOUSE_REPLICAS` | 1 | Number of ClickHouse instances |
| `CLICKHOUSE_CPUS` | 1.0 | CPU limit per ClickHouse container |
| `CLICKHOUSE_MEMORY` | 1g | Memory limit per ClickHouse container |
| `CLICKHOUSE_PASSWORD` | clickhouse123 | ClickHouse password |
| `MYSQL_ROOT_PASSWORD` | root123 | MariaDB root password |
| `QUEUE_BATCH_SIZE` | 500 | Kafka consumer batch size |
| `QUEUE_FLUSH_INTERVAL_MS` | 2000 | Kafka consumer flush interval (ms) |

## Customizing the Load Test

Pass CLI arguments through `run.sh`:

```bash
# Custom worker count and record volume
./run.sh --workers 20 --total-records 100000 --batch-size 1000

# Quick smoke test
./run.sh --workers 3 --total-records 5000 --query-iterations 5

# Heavy stress test (increase VM_MEMORY in .env first)
./run.sh --workers 50 --total-records 500000 --batch-size 1000 --query-iterations 50
```

### CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--backend` | http://localhost:8080 | Backend URL (inside VM) |
| `--workers` | 10 | Concurrent workers per signal type |
| `--total-records` | 50000 | Total records per signal (metrics/logs/traces) |
| `--batch-size` | 500 | Records per OTLP HTTP request |
| `--query-iterations` | 20 | Times each query endpoint is called |
| `--report-dir` | reports | Directory for JSON report output |

## Managing the VM

```bash
# Check VM status
multipass info obs-loadtest

# Shell into the VM
multipass shell obs-loadtest

# View docker logs inside VM
multipass exec obs-loadtest -- bash -c "cd /home/ubuntu/project/loadtest && docker compose logs backend --tail 50"

# Stop the VM (keep it for later runs)
multipass stop obs-loadtest

# Delete the VM completely
multipass delete obs-loadtest && multipass purge

# Re-run against an existing VM (skips provisioning if VM exists)
./run.sh
```

## Reading the Report

Reports are copied to `loadtest/reports/report_<timestamp>.json` on your host.

### JSON Structure

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "environment": {
    "backend_url": "http://localhost:8080",
    "workers": 10,
    "total_records_per_signal": 50000,
    "batch_size": 500,
    "query_iterations": 20
  },
  "ingestion": {
    "metrics": {
      "total_records": 50000,
      "total_requests": 1000,
      "throughput_rps": 850.3,
      "latency_p50_ms": 45.2,
      "latency_p95_ms": 120.3,
      "latency_p99_ms": 250.1,
      "errors": 0,
      "error_rate": 0.0
    }
  },
  "queries": {
    "GET /api/dashboard/overview": {
      "iterations": 20,
      "latency_p50_ms": 125.3,
      "latency_p95_ms": 245.1,
      "latency_p99_ms": 310.2,
      "errors": 0,
      "error_rate": 0.0,
      "avg_response_bytes": 2150
    }
  }
}
```

### Key Metrics to Watch

- **Ingestion throughput_rps**: records/sec — higher is better
- **Ingestion latency_p95_ms**: 95th percentile write latency — should be < 500ms
- **Query latency_p99_ms**: worst-case query time — determines user experience
- **Error rate**: anything > 0% needs investigation

## Tuning Guide

Based on report results, here's what to tune:

### High Ingestion Latency (p95 > 500ms)

1. **Increase Kafka batch size**: `QUEUE_BATCH_SIZE=1000` reduces ClickHouse write frequency
2. **Add backend replicas**: `BACKEND_REPLICAS=4` distributes ingestion load
3. **Increase ClickHouse memory**: `CLICKHOUSE_MEMORY=4g` improves write buffer capacity
4. **Check ClickHouse MergeTree partitions**: monthly partitioning may cause large merges; consider daily partitioning for high-volume deployments

### Slow Query Latency (p95 > 1s)

1. **ClickHouse memory**: Increase `CLICKHOUSE_MEMORY` for larger working sets
2. **ClickHouse CPUs**: Increase `CLICKHOUSE_CPUS` for parallel query execution
3. **Index granularity**: The default `index_granularity = 8192` may be too coarse. Smaller values (4096, 2048) speed up point lookups but increase index memory
4. **Query-level**: Check which endpoints are slow and investigate the underlying ClickHouse queries (add timing/EXPLAIN to repository functions)
5. **Materialized views**: For frequently queried aggregations, consider ClickHouse materialized views for pre-aggregated data

### High Error Rates

1. **Check container logs**: `multipass exec obs-loadtest -- bash -c "cd /home/ubuntu/project/loadtest && docker compose logs backend --tail 100"`
2. **ClickHouse connection pool**: Backend uses MaxOpenConns=20, MaxIdleConns=10. May need tuning under heavy concurrent queries
3. **Kafka consumer lag**: If ingestion errors spike, Kafka may be backed up. Increase `QUEUE_BATCH_SIZE` or add consumer goroutines

### Storage Optimization

1. **TTL policy**: Default is 30 days. Adjust in `db/clickhouse_schema.sql` based on actual retention needs
2. **Compression**: ClickHouse MergeTree uses LZ4 by default. For cold data, consider ZSTD: `ALTER TABLE spans MODIFY COLUMN attributes CODEC(ZSTD(3))`
3. **Partition pruning**: Ensure queries always include time range filters (`startTime`/`endTime`) to leverage monthly partitioning

## Comparing Runs

Keep reports from different configurations and compare:

```bash
# Run with 2 backend replicas (default)
./run.sh --report-dir reports/2-replicas

# Edit .env: BACKEND_REPLICAS=4, then re-run
# (delete VM first for clean state, or it reuses existing)
multipass delete obs-loadtest && multipass purge
./run.sh --report-dir reports/4-replicas

# Compare JSON files
diff <(jq '.ingestion' reports/2-replicas/report_*.json) \
     <(jq '.ingestion' reports/4-replicas/report_*.json)
```

## Reproducing Results

For consistent benchmarks:
1. **Always delete the VM** before re-running (`multipass delete obs-loadtest && multipass purge`)
2. **Use the same `.env`** parameters across runs you want to compare
3. **Close resource-heavy apps** on your host — while the VM is isolated, the hypervisor still shares physical resources
4. **Run multiple times** and average the results — even with a VM, there's some variance
