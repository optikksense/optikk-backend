#!/usr/bin/env python3
"""Load test for the observability backend — ingestion + query benchmarking."""

import argparse
import concurrent.futures
import json
import math
import os
import random
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SERVICES = [
    "checkout-service", "payment-service", "catalog-service",
    "inventory-service", "auth-service",
]
OPERATIONS = [("GET", "/api/data"), ("POST", "/api/submit")]
LOG_LEVELS = ["DEBUG", "INFO", "WARN", "ERROR"]
LOG_LEVEL_WEIGHTS = [5, 70, 15, 10]

# Whitelisted metric names (must match modules/ingestion/store/impl/repository.go)
WHITELISTED_METRICS = [
    "http.server.requests",           # histogram
    "system.cpu.utilization",         # gauge
    "system.memory.utilization",      # gauge
    "thread.pool.active",             # gauge
    "queue.depth",                    # gauge
    "db.connection_pool.utilization", # gauge
    "messaging.kafka.consumer.lag",   # gauge
]


class LoadTestConfig:
    def __init__(self, **kwargs):
        self.backend_url = kwargs.get("backend_url", "http://localhost:8080")
        self.email = kwargs.get("email", "loadtest@observability.local")
        self.password = kwargs.get("password", "LoadTest@12345")
        self.name = kwargs.get("name", "Load Test User")
        self.team_name = kwargs.get("team_name", "Load Test Team")
        self.workers = kwargs.get("workers", 10)
        self.total_records_per_signal = kwargs.get("total_records_per_signal", 50_000)
        self.batch_size = kwargs.get("batch_size", 500)
        self.query_iterations = kwargs.get("query_iterations", 20)


# ---------------------------------------------------------------------------
# Thread-safe metrics collector
# ---------------------------------------------------------------------------

def percentile(sorted_values, p):
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    return sorted_values[int(f)] * (c - k) + sorted_values[int(c)] * (k - f)


class MetricsCollector:
    def __init__(self):
        self._lock = threading.Lock()
        self._ingestion = {}  # signal -> [{"latency_ms", "count", "error"}]
        self._queries = {}    # endpoint -> [{"latency_ms", "status", "size", "error"}]

    def record_ingestion(self, signal, latency_ms, count, error=None):
        with self._lock:
            self._ingestion.setdefault(signal, []).append({
                "latency_ms": latency_ms, "count": count, "error": error,
            })

    def record_query(self, endpoint, latency_ms, status, size, error=None):
        with self._lock:
            self._queries.setdefault(endpoint, []).append({
                "latency_ms": latency_ms, "status": status, "size": size,
                "error": error,
            })

    def ingestion_summary(self):
        result = {}
        with self._lock:
            for signal, records in self._ingestion.items():
                latencies = sorted(r["latency_ms"] for r in records)
                total_count = sum(r["count"] for r in records)
                errors = sum(1 for r in records if r["error"])
                total_time = sum(r["latency_ms"] for r in records) / 1000.0
                result[signal] = {
                    "total_records": total_count,
                    "total_requests": len(records),
                    "throughput_rps": round(total_count / max(total_time, 0.001), 1),
                    "latency_p50_ms": round(percentile(latencies, 50), 1),
                    "latency_p95_ms": round(percentile(latencies, 95), 1),
                    "latency_p99_ms": round(percentile(latencies, 99), 1),
                    "errors": errors,
                    "error_rate": round(errors / max(len(records), 1) * 100, 2),
                }
        return result

    def query_summary(self):
        result = {}
        with self._lock:
            for endpoint, records in self._queries.items():
                latencies = sorted(r["latency_ms"] for r in records)
                errors = sum(1 for r in records if r["error"])
                avg_size = sum(r["size"] for r in records) / max(len(records), 1)
                result[endpoint] = {
                    "iterations": len(records),
                    "latency_p50_ms": round(percentile(latencies, 50), 1),
                    "latency_p95_ms": round(percentile(latencies, 95), 1),
                    "latency_p99_ms": round(percentile(latencies, 99), 1),
                    "errors": errors,
                    "error_rate": round(errors / max(len(records), 1) * 100, 2),
                    "avg_response_bytes": int(avg_size),
                }
        return result


# ---------------------------------------------------------------------------
# OTLP helpers
# ---------------------------------------------------------------------------

def otlp_val(v):
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        return {"intValue": str(v)}
    if isinstance(v, float):
        return {"doubleValue": v}
    return {"stringValue": str(v)}


def otlp_attrs(d):
    return [{"key": k, "value": otlp_val(v)} for k, v in sorted(d.items()) if v is not None]


def utc_ns(dt):
    return str(int(dt.replace(tzinfo=timezone.utc).timestamp() * 1e9))


# ---------------------------------------------------------------------------
# OTLP Data Generator
# ---------------------------------------------------------------------------

class OTLPDataGenerator:
    def __init__(self, team_uuid):
        self.team_uuid = team_uuid

    def _resource(self, svc):
        return {"attributes": otlp_attrs({
            "service.name": svc,
            "team.id": self.team_uuid,
            "deployment.environment": "loadtest",
        })}

    def generate_metrics_batch(self, batch_size, rng):
        """Generate a batch of OTLP metrics using whitelisted names."""
        now = datetime.utcnow()
        by_svc = {}
        for i in range(batch_size):
            svc = rng.choice(SERVICES)
            method, route = rng.choice(OPERATIONS)
            ts = now - timedelta(seconds=rng.randint(0, 3600))
            host = f"{svc}-host"
            pod = f"{svc}-pod-{rng.randint(0, 3)}"

            common_attrs = {
                "service.name": svc, "http.method": method, "http.route": route,
                "server.address": host, "k8s.pod.name": pod,
            }

            metric_name = rng.choice(WHITELISTED_METRICS)
            if metric_name == "http.server.requests":
                count = rng.randint(1, 50)
                total_lat = sum(rng.uniform(10, 200) for _ in range(count))
                metric = {
                    "name": metric_name, "unit": "ms",
                    "histogram": {"dataPoints": [{
                        "startTimeUnixNano": utc_ns(ts - timedelta(minutes=1)),
                        "timeUnixNano": utc_ns(ts),
                        "count": str(count), "sum": total_lat,
                        "attributes": otlp_attrs(common_attrs),
                    }]},
                }
            else:
                metric = {
                    "name": metric_name,
                    "gauge": {"dataPoints": [{
                        "timeUnixNano": utc_ns(ts),
                        "asDouble": rng.uniform(0.05, 0.95),
                        "attributes": otlp_attrs(common_attrs),
                    }]},
                }

            by_svc.setdefault(svc, []).append(metric)

        resource_metrics = []
        for svc, metrics in by_svc.items():
            resource_metrics.append({
                "resource": self._resource(svc),
                "scopeMetrics": [{"scope": {"name": "load_test"}, "metrics": metrics}],
            })
        return {"resourceMetrics": resource_metrics}

    def generate_logs_batch(self, batch_size, rng):
        """Generate a batch of OTLP log records."""
        now = datetime.utcnow()
        by_svc = {}
        for i in range(batch_size):
            svc = rng.choice(SERVICES)
            method, route = rng.choice(OPERATIONS)
            ts = now - timedelta(seconds=rng.randint(0, 3600))
            level = rng.choices(LOG_LEVELS, weights=LOG_LEVEL_WEIGHTS, k=1)[0]
            trace_id = uuid.uuid4().hex
            span_id = uuid.uuid4().hex[:16]

            record = {
                "timeUnixNano": utc_ns(ts),
                "severityText": level,
                "body": {"stringValue": f"[{level}] {method} {route} from {svc}"},
                "traceId": trace_id,
                "spanId": span_id,
                "attributes": otlp_attrs({
                    "service.name": svc, "http.method": method, "http.route": route,
                    "server.address": f"{svc}-host",
                    "k8s.pod.name": f"{svc}-pod-{rng.randint(0, 3)}",
                }),
            }
            by_svc.setdefault(svc, []).append(record)

        resource_logs = []
        for svc, logs in by_svc.items():
            resource_logs.append({
                "resource": self._resource(svc),
                "scopeLogs": [{"scope": {"name": "load_test"}, "logRecords": logs}],
            })
        return {"resourceLogs": resource_logs}

    def generate_traces_batch(self, batch_size, rng):
        """Generate a batch of OTLP trace spans (parent + child)."""
        now = datetime.utcnow()
        by_svc = {}
        count = 0
        while count < batch_size:
            svc = rng.choice(SERVICES)
            method, route = rng.choice(OPERATIONS)
            ts = now - timedelta(seconds=rng.randint(0, 3600))
            trace_id = uuid.uuid4().hex
            span_id = uuid.uuid4().hex[:16]
            lat = rng.uniform(10, 300)
            err = rng.random() < 0.05
            host = f"{svc}-host"
            pod = f"{svc}-pod-{rng.randint(0, 3)}"

            common = {
                "service.name": svc, "http.method": method, "http.route": route,
                "server.address": host, "k8s.pod.name": pod,
            }

            # Parent span
            parent = {
                "traceId": trace_id, "spanId": span_id,
                "name": f"{method} {route}", "kind": 2,  # SERVER
                "startTimeUnixNano": utc_ns(ts),
                "endTimeUnixNano": utc_ns(ts + timedelta(milliseconds=lat)),
                "attributes": otlp_attrs(common),
                "status": {"code": 2 if err else 0},
            }
            by_svc.setdefault(svc, []).append(parent)
            count += 1

            # Optionally add a child DB span
            if rng.random() > 0.4 and count < batch_size:
                child_span_id = uuid.uuid4().hex[:16]
                db_lat = rng.uniform(5, lat * 0.6)
                child = {
                    "traceId": trace_id, "spanId": child_span_id,
                    "parentSpanId": span_id,
                    "name": f"SQL SELECT * FROM {svc.split('-')[0]}_data",
                    "kind": 3,  # CLIENT
                    "startTimeUnixNano": utc_ns(ts + timedelta(milliseconds=2)),
                    "endTimeUnixNano": utc_ns(ts + timedelta(milliseconds=2 + db_lat)),
                    "attributes": otlp_attrs({**common, "db.system": "mysql"}),
                    "status": {"code": 0},
                }
                by_svc.setdefault(svc, []).append(child)
                count += 1

        resource_spans = []
        for svc, spans in by_svc.items():
            resource_spans.append({
                "resource": self._resource(svc),
                "scopeSpans": [{"scope": {"name": "load_test"}, "spans": spans}],
            })
        return {"resourceSpans": resource_spans}


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def signup(config):
    """Create or login test user, return (user_id, team_id, api_key)."""
    resp = requests.post(f"{config.backend_url}/api/signup", json={
        "email": config.email, "password": config.password,
        "name": config.name, "teamName": config.team_name,
    }, timeout=10)

    if resp.status_code == 200:
        data = resp.json()["data"]
        return int(data["user_id"]), int(data["team_id"]), data["api_key"]

    if resp.status_code == 400:
        # User exists — login and fetch API key
        login_resp = requests.post(f"{config.backend_url}/api/auth/login", json={
            "email": config.email, "password": config.password,
        }, timeout=10)
        login_resp.raise_for_status()
        login_data = login_resp.json()["data"]
        token = login_data["token"]
        user_id = int(login_data["user"]["id"])

        teams_resp = requests.get(f"{config.backend_url}/api/teams/my-teams",
                                  headers={"Authorization": f"Bearer {token}"}, timeout=10)
        teams_resp.raise_for_status()
        teams = teams_resp.json()["data"]
        if not teams:
            raise RuntimeError("User has no teams")
        team = teams[0]
        team_id = int(team.get("id") or team.get("teamId") or team.get("team_id"))
        api_key = team.get("api_key") or team.get("apiKey") or ""
        if not api_key:
            raise RuntimeError("Team has no API key")
        return user_id, team_id, api_key

    raise RuntimeError(f"Signup failed: {resp.status_code} {resp.text}")


def login(config):
    """Login and return JWT token."""
    resp = requests.post(f"{config.backend_url}/api/auth/login", json={
        "email": config.email, "password": config.password,
    }, timeout=10)
    resp.raise_for_status()
    return resp.json()["data"]["token"]


# ---------------------------------------------------------------------------
# Phase 1: Ingestion load test
# ---------------------------------------------------------------------------

class IngestionLoadTest:
    def __init__(self, config, api_key, team_uuid, collector):
        self.config = config
        self.api_key = api_key
        self.team_uuid = team_uuid
        self.collector = collector
        self.generator = OTLPDataGenerator(team_uuid)

    def _worker(self, worker_id, signal_type, records_to_send):
        rng = random.Random(worker_id * 1000 + hash(signal_type))
        session = requests.Session()
        session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        })

        url_map = {
            "metrics": f"{self.config.backend_url}/otlp/v1/metrics",
            "logs": f"{self.config.backend_url}/otlp/v1/logs",
            "traces": f"{self.config.backend_url}/otlp/v1/traces",
        }
        url = url_map[signal_type]
        gen_map = {
            "metrics": self.generator.generate_metrics_batch,
            "logs": self.generator.generate_logs_batch,
            "traces": self.generator.generate_traces_batch,
        }
        gen_fn = gen_map[signal_type]

        sent = 0
        while sent < records_to_send:
            batch = min(self.config.batch_size, records_to_send - sent)
            payload = gen_fn(batch, rng)

            start = time.monotonic()
            try:
                resp = session.post(url, json=payload, timeout=30)
                latency = (time.monotonic() - start) * 1000
                accepted = resp.json().get("accepted", 0)
                self.collector.record_ingestion(signal_type, latency, accepted)
                sent += max(accepted, batch)  # advance even if accepted is 0
            except Exception as e:
                latency = (time.monotonic() - start) * 1000
                self.collector.record_ingestion(signal_type, latency, 0, error=str(e))
                sent += batch  # advance to avoid infinite loop

    def run(self):
        records_per_worker = self.config.total_records_per_signal // self.config.workers
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.workers * 3) as pool:
            futures = []
            for signal in ["metrics", "logs", "traces"]:
                for wid in range(self.config.workers):
                    futures.append(pool.submit(self._worker, wid, signal, records_per_worker))
            # Print progress
            done_count = 0
            for f in concurrent.futures.as_completed(futures):
                done_count += 1
                exc = f.exception()
                if exc:
                    print(f"  Worker failed: {exc}")
                if done_count % 5 == 0 or done_count == len(futures):
                    print(f"  Workers completed: {done_count}/{len(futures)}")


# ---------------------------------------------------------------------------
# Phase 2: Query load test
# ---------------------------------------------------------------------------

class QueryLoadTest:
    def __init__(self, config, jwt_token, collector):
        self.config = config
        self.collector = collector
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {jwt_token}"})

    def run(self):
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - 3_600_000  # 1 hour window

        endpoints = [
            ("GET /api/dashboard/overview",
             f"/api/dashboard/overview"),
            ("GET /api/v1/logs",
             f"/api/v1/logs?services=checkout-service&levels=ERROR&startTime={start_ms}&endTime={now_ms}"),
            ("GET /api/v1/logs/histogram",
             f"/api/v1/logs/histogram?startTime={start_ms}&endTime={now_ms}"),
            ("GET /api/v1/traces",
             f"/api/v1/traces?services=checkout-service&startTime={start_ms}&endTime={now_ms}"),
            ("GET /api/v1/services/topology",
             f"/api/v1/services/topology?startTime={start_ms}&endTime={now_ms}"),
            ("GET /api/v1/services/metrics",
             f"/api/v1/services/metrics?startTime={start_ms}&endTime={now_ms}"),
            ("GET /api/v1/services/timeseries",
             f"/api/v1/services/timeseries?startTime={start_ms}&endTime={now_ms}"),
            ("GET /api/v1/metrics/timeseries",
             f"/api/v1/metrics/timeseries?serviceName=checkout-service&startTime={start_ms}&endTime={now_ms}"),
            ("GET /api/v1/latency/histogram",
             f"/api/v1/latency/histogram?serviceName=checkout-service&startTime={start_ms}&endTime={now_ms}"),
        ]

        total = len(endpoints) * self.config.query_iterations
        done = 0
        for name, path in endpoints:
            url = f"{self.config.backend_url}{path}"
            for _ in range(self.config.query_iterations):
                start = time.monotonic()
                try:
                    resp = self.session.get(url, timeout=30)
                    latency = (time.monotonic() - start) * 1000
                    size = len(resp.content)
                    self.collector.record_query(name, latency, resp.status_code, size)
                except Exception as e:
                    latency = (time.monotonic() - start) * 1000
                    self.collector.record_query(name, latency, 0, 0, error=str(e))
                done += 1
            print(f"  {name}: {self.config.query_iterations} iterations done ({done}/{total})")


# ---------------------------------------------------------------------------
# Report generator
# ---------------------------------------------------------------------------

class ReportGenerator:
    def __init__(self, config, collector):
        self.config = config
        self.collector = collector

    def generate(self):
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "environment": {
                "backend_url": self.config.backend_url,
                "workers": self.config.workers,
                "total_records_per_signal": self.config.total_records_per_signal,
                "batch_size": self.config.batch_size,
                "query_iterations": self.config.query_iterations,
            },
            "ingestion": self.collector.ingestion_summary(),
            "queries": self.collector.query_summary(),
        }

    def save_json(self, report_dir="reports"):
        report = self.generate()
        os.makedirs(report_dir, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(report_dir, f"report_{ts}.json")
        with open(path, "w") as f:
            json.dump(report, f, indent=2)
        return path

    def print_summary(self):
        report = self.generate()

        print("\n" + "=" * 90)
        print("  INGESTION RESULTS")
        print("=" * 90)
        hdr = f"{'Signal':<10} {'Records':>10} {'Requests':>10} {'Throughput':>12} {'p50 ms':>9} {'p95 ms':>9} {'p99 ms':>9} {'Errors':>8}"
        print(hdr)
        print("-" * 90)
        for signal, m in report["ingestion"].items():
            print(f"{signal:<10} {m['total_records']:>10} {m['total_requests']:>10} "
                  f"{m['throughput_rps']:>10.1f}/s {m['latency_p50_ms']:>9.1f} "
                  f"{m['latency_p95_ms']:>9.1f} {m['latency_p99_ms']:>9.1f} {m['errors']:>8}")

        print("\n" + "=" * 100)
        print("  QUERY RESULTS")
        print("=" * 100)
        hdr = f"{'Endpoint':<38} {'Iters':>6} {'p50 ms':>9} {'p95 ms':>9} {'p99 ms':>9} {'Errors':>7} {'Avg Size':>10}"
        print(hdr)
        print("-" * 100)
        for ep, m in report["queries"].items():
            size_str = f"{m['avg_response_bytes']}B"
            if m['avg_response_bytes'] > 1024:
                size_str = f"{m['avg_response_bytes'] / 1024:.1f}KB"
            print(f"{ep:<38} {m['iterations']:>6} {m['latency_p50_ms']:>9.1f} "
                  f"{m['latency_p95_ms']:>9.1f} {m['latency_p99_ms']:>9.1f} "
                  f"{m['errors']:>7} {size_str:>10}")
        print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Load test for observability backend")
    parser.add_argument("--backend", default="http://localhost:8080",
                        help="Backend base URL (default: http://localhost:8080)")
    parser.add_argument("--workers", type=int, default=10,
                        help="Concurrent ingestion workers per signal (default: 10)")
    parser.add_argument("--total-records", type=int, default=50_000,
                        help="Total records per signal type (default: 50000)")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Records per OTLP request (default: 500)")
    parser.add_argument("--query-iterations", type=int, default=20,
                        help="Iterations per query endpoint (default: 20)")
    parser.add_argument("--report-dir", default="reports",
                        help="Report output directory (default: reports)")
    args = parser.parse_args()

    config = LoadTestConfig(
        backend_url=args.backend,
        workers=args.workers,
        total_records_per_signal=args.total_records,
        batch_size=args.batch_size,
        query_iterations=args.query_iterations,
    )
    collector = MetricsCollector()

    # Step 1: Signup
    print(f"[1/5] Signing up test user ({config.email})...")
    user_id, team_id, api_key = signup(config)
    team_uuid = f"00000000-0000-0000-0000-{team_id:012d}"
    print(f"  User ID: {user_id}, Team ID: {team_id}")

    # Step 2: Ingestion phase
    print(f"\n[2/5] Ingestion phase: {config.total_records_per_signal} records/signal, "
          f"{config.workers} workers, batch size {config.batch_size}")
    t0 = time.monotonic()
    ingestion = IngestionLoadTest(config, api_key, team_uuid, collector)
    ingestion.run()
    elapsed = time.monotonic() - t0
    print(f"  Ingestion completed in {elapsed:.1f}s")

    # Step 3: Wait for Kafka flush
    flush_wait = 15
    print(f"\n[3/5] Waiting {flush_wait}s for Kafka consumer to flush to ClickHouse...")
    time.sleep(flush_wait)

    # Step 4: Query phase
    print(f"\n[4/5] Query phase: {config.query_iterations} iterations per endpoint")
    jwt_token = login(config)
    queries = QueryLoadTest(config, jwt_token, collector)
    queries.run()

    # Step 5: Report
    print("\n[5/5] Generating report...")
    reporter = ReportGenerator(config, collector)
    path = reporter.save_json(args.report_dir)
    reporter.print_summary()
    print(f"Full report saved to: {path}")


if __name__ == "__main__":
    main()
