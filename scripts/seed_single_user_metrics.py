#!/usr/bin/env python3
"""Concise OTLP Seeding Script for Synthetic Telemetry."""
import argparse, json, os, random, secrets, sys, uuid
from datetime import datetime, timedelta, timezone
from urllib import error, request

# --- Configuration & Defaults ---
DEFAULTS = {
    "email": "frontend.demo@observability.local",
    "password": "Demo@12345",
    "name": "Demo User",
    "team": "Demo Team",
    "samples": 20,
    "backend": "http://localhost:8080"
}

def get_endpoint(key, suffix, backend):
    val = (os.getenv(key) or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or "").strip()
    if val:
        base = val.rstrip("/")
        if base.endswith(suffix):
            return base
        if base.endswith("/otlp"):
            return f"{base}{suffix}"
        return f"{base}/otlp{suffix}"
    return f"{backend.rstrip('/')}/otlp{suffix}"

def _int_from(obj, *keys):
    for key in keys:
        val = obj.get(key)
        if val is not None:
            try:
                return int(val)
            except (TypeError, ValueError):
                continue
    return 0

# --- OTLP Helpers ---
def otlp_val(v):
    if isinstance(v, bool): return {"boolValue": v}
    if isinstance(v, (int, float)): return {"intValue": str(int(v))} if isinstance(v, int) else {"doubleValue": float(v)}
    return {"stringValue": str(v)}

def otlp_attrs(d):
    return [{"key": k, "value": otlp_val(v)} for k, v in sorted(d.items()) if v is not None]

def utc_ns(dt):
    return str(int(dt.replace(tzinfo=timezone.utc).timestamp() * 1e9))

def build_payload(signal, key, items, team_uuid):
    res = []
    by_service = {}
    for item in items:
        svc = item.get("_svc", "infrastructure")
        by_service.setdefault(svc, []).append(item)
    
    for svc, itms in sorted(by_service.items()):
        for itm in itms: itm.pop("_svc", None)
        s_key = "scopeSpans" if signal == "trace" else f"scope{signal.capitalize()}s"
        res.append({
            "resource": {"attributes": otlp_attrs({"service.name": svc, "team.id": team_uuid, "deployment.environment": "local"})},
            s_key: [{"scope": {"name": "seed_script"}, f"{key}": itms}]
        })
    p_key = "resourceSpans" if signal == "trace" else f"resource{signal.capitalize()}s"
    return {p_key: res}

# --- Actions ---
def signup(base_url, email, pwd, name, team):
    try:
        req = request.Request(f"{base_url}/api/signup", data=json.dumps({"email":email, "password":pwd, "name":name, "teamName":team}).encode(), headers={"Content-Type":"application/json"})
        with request.urlopen(req, timeout=10) as r:
            res = json.loads(r.read())["data"]
            return int(res["user_id"]), int(res["team_id"]), res["api_key"]
    except error.HTTPError as e:
        if e.code == 400:
            req = request.Request(f"{base_url}/api/auth/login", data=json.dumps({"email":email, "password":pwd}).encode(), headers={"Content-Type":"application/json"})
            with request.urlopen(req, timeout=10) as r:
                login_data = json.loads(r.read())["data"]
                token = login_data["token"]
                user_id = int(login_data["user"]["id"])
                current_team_id = _int_from(login_data.get("currentTeam", {}), "id", "teamId", "team_id")
                req_t = request.Request(f"{base_url}/api/teams/my-teams", headers={"Authorization": f"Bearer {token}"})
                with request.urlopen(req_t, timeout=10) as rt:
                    teams = json.loads(rt.read())["data"]
                    if not teams: raise RuntimeError("User has no teams")
                    selected = None
                    if current_team_id:
                        for t in teams:
                            if _int_from(t, "id", "teamId", "team_id") == current_team_id:
                                selected = t
                                break
                    if selected is None:
                        selected = teams[0]

                    team_id = _int_from(selected, "id", "teamId", "team_id")
                    api_key = selected.get("api_key") or selected.get("apiKey") or ""
                    if not team_id or not api_key:
                        raise RuntimeError("Selected team is missing id/api_key")
                    return user_id, team_id, api_key
        raise RuntimeError(f"Signup failed: {e}")
    except Exception as e: raise RuntimeError(f"Signup failed: {e}")

def post_otlp(url, key, payload, signal):
    try:
        req = request.Request(url, data=json.dumps(payload).encode(), headers={"Authorization":f"Bearer {key}", "Content-Type":"application/json"})
        with request.urlopen(req, timeout=30) as r:
            res = json.loads(r.read())
            count = res.get("accepted")
            if count is None:
                count = res.get("inserted", 0)
            return int(count)
    except Exception as e: raise RuntimeError(f"OTLP {signal} failed: {e}")

def generate_data(samples, team_uuid):
    rng = random.Random(42)
    now = datetime.utcnow()
    metrics, logs, traces = [], [], []
    
    svcs = ["checkout-service", "payment-service", "catalog-service", 
            "inventory-service", "auth-service"]
    ops = [("GET", "/api/data"), ("POST", "/api/submit")]

    for i in range(samples):
        ts = now - timedelta(minutes=(samples - i))
        for svc in svcs:
            host, pod = f"{svc}-host", f"{svc}-pod"
            container = f"{svc}-container"
            queue_name = f"{svc}.events"
            
            for method, route in ops:
                # Random number of requests for this endpoint at this timestamp
                num_requests = rng.randint(1, 20)          # vary as needed
                msg_op = "publish" if method == "POST" else "receive"
                span_kind = 4 if msg_op == "publish" else 5
                
                total_latency = 0.0
                error_count = 0
                
                for _ in range(num_requests):
                    # Per‑request attributes
                    err = rng.random() < 0.05
                    lat = rng.uniform(10, 200)
                    total_latency += lat
                    if err:
                        error_count += 1
                    
                    trace_id = uuid.uuid4().hex
                    span_id = uuid.uuid4().hex[:16]
                    
                    common = {
                        "service.name": svc, "http.method": method, "http.route": route,
                        "server.address": host, "k8s.pod.name": pod,
                        "k8s.container.name": container,
                        "messaging.queue.name": queue_name, "messaging.operation": msg_op,
                        "db.connection_pool.utilization": rng.uniform(20, 90),
                        "messaging.kafka.consumer.lag": rng.randint(0, 2000),
                        "thread.pool.active": rng.randint(10, 150),
                        "thread.pool.size": 200,
                        "queue.depth": rng.randint(0, 500),
                        "system.cpu.utilization": rng.uniform(10, 90),
                        "system.memory.utilization": rng.uniform(10, 90),
                        "system.disk.utilization": rng.uniform(5, 80),
                        "system.network.utilization": rng.uniform(1, 50),
                        "_svc": svc
                    }
                    
                    # Decide if this request has a DB call and/or cache access
                    has_db = rng.random() > 0.3
                    has_cache = rng.random() > 0.5
                    
                    # Trace span
                    primary_name = f"{method} {route}"
                    primary_duration = lat
                    if has_db:
                        # Make parent span longer
                        primary_duration += rng.uniform(10, 50)
                        db_lat = rng.uniform(5, primary_duration - 5)
                        db_span_id = uuid.uuid4().hex[:16]
                        db_table = f"{svc.split('-')[0]}_data"
                        db_attrs = common.copy()
                        db_attrs.update({
                            "db.system": "mysql",
                            "db.sql.table": db_table,
                            "db.query.latency.ms": db_lat,
                            "db.replication.lag.ms": rng.uniform(0, 100) if rng.random() > 0.8 else 0
                        })
                        if has_cache:
                            db_attrs["cache.hit"] = "true" if rng.random() > 0.4 else "false"
                            
                        traces.append({
                            "traceId": trace_id, "spanId": db_span_id, "parentSpanId": span_id,
                            "name": f"SQL SELECT * FROM {db_table}", "kind": 3, # CLIENT
                            "startTimeUnixNano": utc_ns(ts + timedelta(milliseconds=2)),
                            "endTimeUnixNano": utc_ns(ts + timedelta(milliseconds=2+db_lat)),
                            "attributes": otlp_attrs(db_attrs),
                            "status": {"code": 0},
                            "_svc": svc
                        })

                    traces.append({
                        "traceId": trace_id, "spanId": span_id,
                        "name": f"{method} {route}", "kind": span_kind,
                        "startTimeUnixNano": utc_ns(ts),
                        "endTimeUnixNano": utc_ns(ts + timedelta(milliseconds=lat)),
                        "attributes": otlp_attrs(common),
                        "status": {"code": 2 if err else 0},
                        "_svc": svc
                    })
                    
                    # Log record
                    logs.append({
                        "timeUnixNano": utc_ns(ts),
                        "severityText": "ERROR" if err else "INFO",
                        "body": {"stringValue": f"Request {method} {route}"},
                        "traceId": trace_id, "spanId": span_id,
                        "attributes": otlp_attrs(common),
                        "_svc": svc
                    })
                
                # Aggregated metrics for this endpoint at this timestamp
                common_metric = {
                    "service.name": svc, "http.method": method, "http.route": route,
                    "server.address": host, "k8s.pod.name": pod,
                    "k8s.container.name": container,
                    "messaging.queue.name": queue_name, "messaging.operation": msg_op,
                    "_svc": svc
                }
                
                # Histogram (http.server.requests)
                metrics.append({
                    "name": "http.server.requests", "unit": "ms",
                    "histogram": {
                        "dataPoints": [{
                            "startTimeUnixNano": utc_ns(ts - timedelta(minutes=1)),
                            "timeUnixNano": utc_ns(ts),
                            "count": str(num_requests),
                            "sum": total_latency,
                            "attributes": otlp_attrs(common_metric)
                        }]
                    },
                    "_svc": svc
                })
                
                # Gauge (system.cpu.utilization) – one per service per timestamp (not per endpoint)
                # To keep it simple, we still add one gauge per service per timestamp,
                # but you can move it outside the endpoint loop if you prefer.
                metrics.append({
                    "name": "system.cpu.utilization",
                    "gauge": {
                        "dataPoints": [{
                            "timeUnixNano": utc_ns(ts),
                            "asDouble": rng.uniform(0.1, 0.9),
                            "attributes": otlp_attrs({**common_metric, "cpu.state": "user"})
                        }]
                    },
                    "_svc": svc
                })
                
                # Sum (http.server.requests.count)
                metrics.append({
                    "name": "http.server.requests.count",
                    "sum": {
                        "isMonotonic": True,
                        "dataPoints": [{
                            "timeUnixNano": utc_ns(ts),
                            "asInt": str(num_requests),
                            "attributes": otlp_attrs(common_metric)
                        }]
                    },
                    "_svc": svc
                })
                
                # Optionally add error count metric
                if error_count > 0:
                    metrics.append({
                        "name": "http.server.requests.error_count",
                        "sum": {
                            "isMonotonic": True,
                            "dataPoints": [{
                                "timeUnixNano": utc_ns(ts),
                                "asInt": str(error_count),
                                "attributes": otlp_attrs(common_metric)
                            }]
                        },
                        "_svc": svc
                    })
    
    return metrics, logs, traces

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--email", default=DEFAULTS["email"])
    p.add_argument("--samples", type=int, default=DEFAULTS["samples"])
    p.add_argument("--backend", default=DEFAULTS["backend"])
    args = p.parse_args()

    print(f"Signing up {args.email}...")
    uid, tid, akey = signup(args.backend, args.email, DEFAULTS["password"], DEFAULTS["name"], DEFAULTS["team"])
    tuuid = f"00000000-0000-0000-0000-{tid:012d}"
    metrics_ep = get_endpoint("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "/v1/metrics", args.backend)
    logs_ep = get_endpoint("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "/v1/logs", args.backend)
    traces_ep = get_endpoint("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "/v1/traces", args.backend)
    
    print(f"Generating and sending {args.samples} samples...")
    m, l, t = generate_data(args.samples, tuuid)
    
    res = {
        "metrics": post_otlp(metrics_ep, akey, build_payload("metric", "metrics", m, tuuid), "metrics"),
        "logs": post_otlp(logs_ep, akey, build_payload("log", "logRecords", l, tuuid), "logs"),
        "traces": post_otlp(traces_ep, akey, build_payload("trace", "spans", t, tuuid), "traces")
    }
    
    print(f"\n✓ Seed Match!\nMetrics: {res['metrics']}, Logs: {res['logs']}, Traces: {res['traces']}")
    print(f"Email: {args.email}\nPassword: {DEFAULTS['password']}\nTeam UUID: {tuuid}\nAPI Key: {akey}")

if __name__ == "__main__": main()
