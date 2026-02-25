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
            return int(res.get("inserted", 0))
    except Exception as e: raise RuntimeError(f"OTLP {signal} failed: {e}")

# --- Generation ---
def generate_data(samples, team_uuid):
    rng = random.Random(42)
    now = datetime.utcnow()
    metrics, logs, traces = [], [], []
    
    svcs = ["checkout-service", "payment-service", "catalog-service", "inventory-service", "auth-service"]
    ops = [("GET", "/api/data"), ("POST", "/api/submit")]

    for i in range(samples):
        ts = now - timedelta(minutes=(samples - i))
        for svc in svcs:
            h, p = f"{svc}-host", f"{svc}-pod"
            for m, r in ops:
                count = rng.randint(10, 100)
                err = rng.random() < 0.05
                lat = rng.uniform(10, 200)
                t_id, s_id = uuid.uuid4().hex, uuid.uuid4().hex[:16]
                c = f"{svc}-container"
                qn = f"{svc}.events"
                msg_op = "publish" if m == "POST" else "receive"
                span_kind = 4 if msg_op == "publish" else 5
                
                common = {
                    "service.name": svc, "http.method": m, "http.route": r, 
                    "server.address": h, "k8s.pod.name": p, "k8s.container.name": c,
                    "messaging.queue.name": qn, "messaging.operation": msg_op,
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
                
                # Metrics (Histogram, Gauge, Sum)
                metrics.append({"name": "http.server.requests", "unit": "ms", "histogram": {"dataPoints": [{
                    "startTimeUnixNano": utc_ns(ts-timedelta(minutes=1)), "timeUnixNano": utc_ns(ts), "count": str(count), "sum": lat*count, "attributes": otlp_attrs(common)
                }]}, "_svc": svc})
                metrics.append({"name": "system.cpu.utilization", "gauge": {"dataPoints": [{
                    "timeUnixNano": utc_ns(ts), "asDouble": rng.uniform(0.1, 0.9), "attributes": otlp_attrs({**common, "cpu.state": "user"})
                }]}, "_svc": svc})
                metrics.append({"name": "http.server.requests.count", "sum": {"isMonotonic": True, "dataPoints": [{
                    "timeUnixNano": utc_ns(ts), "asInt": str(count), "attributes": otlp_attrs(common)
                }]}, "_svc": svc})
                
                # Trace & Log
                traces.append({"traceId": t_id, "spanId": s_id, "name": f"{m} {r}", "kind": span_kind, "startTimeUnixNano": utc_ns(ts), "endTimeUnixNano": utc_ns(ts+timedelta(milliseconds=lat)), "attributes": otlp_attrs(common), "status": {"code": 2 if err else 0}, "_svc": svc})
                logs.append({"timeUnixNano": utc_ns(ts), "severityText": "ERROR" if err else "INFO", "body": {"stringValue": f"Request {m} {r}"}, "traceId": t_id, "spanId": s_id, "attributes": otlp_attrs(common), "_svc": svc})

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
