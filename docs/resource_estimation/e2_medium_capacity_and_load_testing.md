# Capacity Estimation: GCP e2-medium & Load Testing Guide

## Audit Date: 2026-03-10

---

## 1. GCP e2-medium Specs

| Resource | Value |
|----------|-------|
| vCPUs | 2 (shared-core, burstable) |
| Memory | 4 GB |
| Network | Up to 4 Gbps (burstable) |
| Disk | Depends on attached PD (typically 10-50GB SSD) |
| CPU credit | Burstable - sustained workload throttled to ~50% of 2 vCPUs |

**Key constraint**: e2-medium is a shared-core VM. Under sustained load, CPU is throttled. Burst capacity is temporary (~30 minutes of full 2 vCPU, then throttled).

---

## 2. Component Resource Consumption

### 2.1 Go Backend (this app)

| Component | CPU | Memory | Notes |
|-----------|-----|--------|-------|
| HTTP server (Gin) | Low | ~50MB base | Per-request goroutine |
| gRPC server | Low-Med | ~80MB base | Proto deserialization |
| Ingest queue (3 queues) | Med | ~30-100MB | Buffered rows in memory |
| Attribute processing | High | ~20MB/batch | Main CPU bottleneck |
| JSON serialization | Med | Transient | GC pressure |
| DB connection pools | Low | ~5MB | 50+50 connections |
| **Total (idle)** | - | **~200MB** | |
| **Total (under load)** | - | **~500MB-1GB** | |

### 2.2 ClickHouse (if co-located)

| Component | CPU | Memory | Notes |
|-----------|-----|--------|-------|
| Idle | Low | ~500MB | Buffer pools, caches |
| Query execution | High | 1-4GB | Aggregations, JOINs |
| Ingest (MergeTree merges) | Med | ~500MB | Background merges |
| **Total** | - | **2-4GB** | Minimum recommended: 4GB |

### 2.3 MySQL (if co-located)

| Component | CPU | Memory | Notes |
|-----------|-----|--------|-------|
| InnoDB buffer pool | - | ~128MB | Default config |
| Connections | Low | ~10MB | 50 connections |
| **Total** | - | **~200MB** | |

---

## 3. Deployment Scenarios on e2-medium

### Scenario A: All-in-one (Go + ClickHouse + MySQL on single VM)

**NOT RECOMMENDED for production.**

| Resource | Usage | Available | Status |
|----------|-------|-----------|--------|
| Memory | ~3-5GB | 4GB | EXCEEDED |
| CPU | 3 services competing | 2 vCPU (burstable) | EXCEEDED |

ClickHouse alone needs 4GB for query execution. This setup will OOM.

### Scenario B: Go backend only (ClickHouse + MySQL external)

**Viable for low traffic.**

| Resource | Usage | Available | Headroom |
|----------|-------|-----------|----------|
| Memory (idle) | ~200MB | 4GB | 3.8GB |
| Memory (peak) | ~800MB-1.2GB | 4GB | ~3GB |
| CPU (sustained) | ~0.5-1 vCPU | ~1 vCPU (throttled) | Tight |
| CPU (burst) | ~1.5-2 vCPU | 2 vCPU | OK for 30 min |

### Scenario C: Dockerized Go backend (recommended for e2-medium)

Run Go backend in Docker with resource limits:
```yaml
services:
  backend:
    image: optikk-backend
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.5'
```

---

## 4. Throughput Estimates (Scenario B - Go Backend Only)

### 4.1 Ingest Throughput

| Metric | Current Code | After Phase 1 Fixes |
|--------|-------------|---------------------|
| Spans/sec (sustained) | ~500-1,000 | ~5,000-8,000 |
| Spans/sec (burst) | ~2,000-3,000 | ~10,000-15,000 |
| Logs/sec | ~800-1,500 | ~5,000-10,000 |
| Metrics datapoints/sec | ~1,000-2,000 | ~8,000-12,000 |

**Bottleneck**: CPU for attribute processing (linear lookups, JSON serialization).

**Math**:
- Each span: ~10 attribute lookups (O(n) each) + JSON marshal + queue append
- Current: ~1ms/span on e2-medium => ~1,000 spans/sec/core
- After map optimization: ~0.1-0.2ms/span => ~5,000-10,000 spans/sec/core
- With 1 core for ingest: **~5,000 spans/sec sustained**

### 4.2 Query Throughput (Dashboard API)

| Query Type | Current (cold) | Current (warm) | After PREWHERE |
|-----------|---------------|----------------|----------------|
| Simple aggregation (1h) | 2-5s | 500ms-2s | 200ms-800ms |
| Time-series (1h, 60 buckets) | 3-8s | 1-3s | 500ms-1.5s |
| Service dependencies (JOIN) | 5-15s | 2-5s | 1-3s |
| Trace detail (by trace_id) | 1-3s | 200ms-1s | 100ms-500ms |
| Top-N operations | 3-10s | 1-4s | 500ms-2s |

**Concurrent dashboard users**: ~5-10 users simultaneously before queries queue up.

### 4.3 Combined Load Capacity

On e2-medium running Go backend only (external ClickHouse + MySQL):

| Scenario | Ingest Rate | Concurrent Users | Stable? |
|----------|------------|-------------------|---------|
| Light | 500 spans/sec | 5 users | Yes |
| Medium | 1,000 spans/sec | 3 users | Marginal |
| Heavy | 2,000+ spans/sec | 5+ users | No - CPU saturated |

**Recommendation**: e2-medium can handle a **small staging environment** or a **single microservice deployment** (~5-10 services generating 500-1,000 spans/sec total).

For production with the OpenTelemetry demo app (even at minimal scale), you need:
- **Minimum**: e2-standard-4 (4 vCPU, 16GB) for Go backend
- **ClickHouse**: e2-standard-8 (8 vCPU, 32GB) minimum, or ClickHouse Cloud

---

## 5. Load Testing Guide

### 5.1 Prerequisites

```bash
# Install k6 (load testing tool with OTLP support)
brew install k6

# Install xk6-distributed-tracing extension for OTLP
go install go.k6.io/xk6/cmd/xk6@latest
xk6 build --with github.com/grafana/xk6-distributed-tracing

# OR use telemetrygen (OpenTelemetry load generator)
go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest
```

### 5.2 Test 1: Ingest Throughput (OTLP HTTP)

```bash
# Generate spans at increasing rates using telemetrygen
# Start with 100 spans/sec, increase by 100 every 30 seconds

# Baseline: 100 spans/sec
telemetrygen traces \
  --otlp-http \
  --otlp-http-endpoint="http://localhost:4318" \
  --otlp-attributes='team_api_key="YOUR_API_KEY"' \
  --rate 100 \
  --duration 60s \
  --service "load-test-svc" \
  --span-duration 50ms \
  --child-spans 3

# Medium load: 500 spans/sec
telemetrygen traces \
  --otlp-http \
  --otlp-http-endpoint="http://localhost:4318" \
  --otlp-attributes='team_api_key="YOUR_API_KEY"' \
  --rate 500 \
  --duration 120s \
  --service "load-test-svc" \
  --span-duration 50ms \
  --child-spans 3

# Stress: 2000 spans/sec
telemetrygen traces \
  --otlp-http \
  --otlp-http-endpoint="http://localhost:4318" \
  --otlp-attributes='team_api_key="YOUR_API_KEY"' \
  --rate 2000 \
  --duration 120s \
  --service "load-test-svc" \
  --span-duration 50ms \
  --child-spans 5
```

**What to monitor**:
- `top` / `htop`: CPU usage of Go process
- Queue depth: Add temporary log in `Enqueue()` or expose via `/health`
- HTTP 429 responses: Indicates backpressure triggered
- ClickHouse: `SELECT query_id, read_rows, elapsed FROM system.query_log ORDER BY event_time DESC LIMIT 20`
- Memory: `ps aux | grep optikk` or Docker stats

### 5.3 Test 2: Ingest Throughput (OTLP gRPC)

```bash
# gRPC is more efficient, expect ~20-30% higher throughput
telemetrygen traces \
  --otlp-grpc \
  --otlp-grpc-endpoint="localhost:4317" \
  --otlp-attributes='team_api_key="YOUR_API_KEY"' \
  --rate 1000 \
  --duration 120s \
  --service "load-test-grpc" \
  --span-duration 50ms \
  --child-spans 3 \
  --otlp-insecure
```

### 5.4 Test 3: Dashboard API Query Load

Create a k6 script:

```javascript
// file: load_test_api.js
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = 'http://localhost:9090/api/v1';
const TOKEN = 'YOUR_JWT_TOKEN'; // Get from login API first

const headers = {
  'Authorization': `Bearer ${TOKEN}`,
  'Content-Type': 'application/json',
  'X-Team-Id': 'YOUR_TEAM_UUID',
};

// Time range: last 1 hour
const now = Date.now();
const start = now - 3600000;

export const options = {
  scenarios: {
    // Ramp up dashboard users
    dashboard_load: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '30s', target: 5 },   // ramp to 5 users
        { duration: '60s', target: 5 },   // hold at 5
        { duration: '30s', target: 10 },  // ramp to 10
        { duration: '60s', target: 10 },  // hold at 10
        { duration: '30s', target: 20 },  // ramp to 20 (stress)
        { duration: '60s', target: 20 },  // hold at 20
        { duration: '30s', target: 0 },   // ramp down
      ],
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<3000'], // 95% of requests under 3s
    http_req_failed: ['rate<0.05'],     // less than 5% errors
  },
};

const endpoints = [
  // Overview page
  `/spans/operation-aggregation?start=${start}&end=${now}&limit=20`,
  `/spans/service-scorecard?start=${start}&end=${now}`,
  `/spans/apdex?start=${start}&end=${now}&satisfied_ms=500&tolerating_ms=2000`,

  // RED metrics
  `/spans/top-slow-operations?start=${start}&end=${now}&limit=10`,
  `/spans/top-error-operations?start=${start}&end=${now}&limit=10`,
  `/spans/http-status-distribution?start=${start}&end=${now}`,

  // Error tracking
  `/spans/exception-rate-by-type?start=${start}&end=${now}`,
  `/spans/error-hotspot?start=${start}&end=${now}`,
  `/spans/http-5xx-by-route?start=${start}&end=${now}`,

  // Service map
  `/services/topology?start=${start}&end=${now}`,
];

export default function () {
  // Simulate a user loading a dashboard (hits multiple endpoints)
  const endpoint = endpoints[Math.floor(Math.random() * endpoints.length)];
  const res = http.get(`${BASE_URL}${endpoint}`, { headers });

  check(res, {
    'status is 200': (r) => r.status === 200,
    'response time < 3s': (r) => r.timings.duration < 3000,
  });

  sleep(1 + Math.random() * 2); // 1-3 second think time between requests
}
```

Run:
```bash
k6 run load_test_api.js
```

### 5.5 Test 4: Combined Load (Ingest + Queries)

Run ingest and API load simultaneously:

```bash
# Terminal 1: Ingest load
telemetrygen traces \
  --otlp-http \
  --otlp-http-endpoint="http://localhost:4318" \
  --otlp-attributes='team_api_key="YOUR_API_KEY"' \
  --rate 500 \
  --duration 300s \
  --service "load-test-svc" \
  --child-spans 3

# Terminal 2: API query load
k6 run load_test_api.js

# Terminal 3: Monitor
watch -n 1 'curl -s http://localhost:9090/health | jq .'
```

### 5.6 Test 5: Soak Test (Memory Leaks)

Run moderate load for 2+ hours to detect memory leaks:

```bash
telemetrygen traces \
  --otlp-http \
  --otlp-http-endpoint="http://localhost:4318" \
  --otlp-attributes='team_api_key="YOUR_API_KEY"' \
  --rate 200 \
  --duration 7200s \
  --service "soak-test-svc" \
  --child-spans 2
```

Monitor memory over time:
```bash
# Record memory every 10 seconds
while true; do
  echo "$(date): $(ps -o rss= -p $(pgrep optikk))" >> mem_log.txt
  sleep 10
done
```

If RSS grows continuously, there's a memory leak (likely in ingest queues or attribute maps).

---

## 6. Key Metrics to Collect During Load Tests

| Metric | Tool | Warning Threshold | Critical |
|--------|------|-------------------|----------|
| CPU % | `top`, `htop` | >70% sustained | >90% |
| Memory RSS | `ps`, Docker stats | >2GB (on 4GB VM) | >3GB |
| Go goroutines | pprof `/debug/pprof/goroutine` | >5,000 | >10,000 |
| HTTP 429 rate | k6 output | >1% | >5% |
| API p95 latency | k6 output | >3s | >10s |
| ClickHouse query time | system.query_log | >5s | >30s |
| MySQL connection wait | MySQL processlist | >10 waiting | >25 waiting |
| Ingest queue depth | Application logs | >5,000 | >9,000 |

---

## 7. Go pprof Profiling

Enable pprof (already available in Go via `net/http/pprof`):

```bash
# CPU profile (30 seconds)
go tool pprof http://localhost:9090/debug/pprof/profile?seconds=30

# Memory profile
go tool pprof http://localhost:9090/debug/pprof/heap

# Goroutine dump
curl http://localhost:9090/debug/pprof/goroutine?debug=2 > goroutines.txt

# Trace (5 seconds)
curl -o trace.out http://localhost:9090/debug/pprof/trace?seconds=5
go tool trace trace.out
```

**What to look for**:
- CPU: `lookupAttr`, `attrsToJSON`, `json.Marshal` should be top consumers (confirms optimization targets)
- Memory: Heap growth in `ingest.Queue.buf`, attribute map allocations
- Goroutines: Stuck goroutines waiting on ClickHouse or MySQL connections

---

## 8. Recommended VM Sizing

| Workload | Services | Spans/sec | VM Type | Monthly Cost |
|----------|----------|-----------|---------|-------------|
| Dev/Demo | 5-10 | <500 | e2-medium (2 vCPU, 4GB) | ~$25 |
| Small Staging | 10-20 | 500-2,000 | e2-standard-2 (2 vCPU, 8GB) | ~$49 |
| Production (small) | 20-50 | 2,000-10,000 | e2-standard-4 (4 vCPU, 16GB) | ~$97 |
| Production (medium) | 50-100 | 10,000-50,000 | e2-standard-8 (8 vCPU, 32GB) | ~$194 |
| Production (large) | 100+ | 50,000+ | Multiple pods + HPA | Varies |

**ClickHouse sizing** (separate from app):
| Data Volume | Daily Spans | Storage/month | Recommended |
|-------------|------------|---------------|-------------|
| Small | <10M | ~5GB | e2-standard-4 + 100GB SSD |
| Medium | 10M-100M | ~50GB | e2-standard-8 + 500GB SSD |
| Large | 100M-1B | ~500GB | ClickHouse Cloud or cluster |

---

## 9. Quick Start Checklist

1. [ ] Install `telemetrygen` and `k6`
2. [ ] Start the backend locally or on target VM
3. [ ] Run Test 1 (ingest baseline) at 100, 500, 1000, 2000 spans/sec
4. [ ] Record CPU/memory at each level
5. [ ] Run Test 3 (API query load) with 5, 10, 20 virtual users
6. [ ] Record p95 latency and error rate
7. [ ] Run Test 4 (combined) at your expected production load
8. [ ] Run Test 5 (soak) for 2+ hours
9. [ ] Profile with pprof to identify actual bottlenecks
10. [ ] Compare results before/after applying Phase 1 fixes
