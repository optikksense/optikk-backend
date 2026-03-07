#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./test_telemetry_common.sh
source "${SCRIPT_DIR}/test_telemetry_common.sh"

ENVIRONMENT="staging"
APP_SERVICE=""
DB_SERVICE=""
QUEUE_SERVICE=""
AI_SERVICE=""
HOST_NAME=""
POD_NAME=""
CONTAINER_NAME=""
NODE_NAME=""
NAMESPACE_NAME=""
QUEUE_NAME=""
AI_MODEL_PRIMARY="gpt-4o-mini"
AI_MODEL_SECONDARY="gpt-4.1"

init_metric_context() {
  APP_SERVICE="metrics-app-${TEST_RUN_ID}"
  DB_SERVICE="metrics-db-${TEST_RUN_ID}"
  QUEUE_SERVICE="metrics-queue-${TEST_RUN_ID}"
  AI_SERVICE="metrics-ai-${TEST_RUN_ID}"
  HOST_NAME="metrics-host-${TEST_RUN_ID}"
  POD_NAME="metrics-pod-${TEST_RUN_ID}"
  CONTAINER_NAME="metrics-container-${TEST_RUN_ID}"
  NODE_NAME="metrics-node-${TEST_RUN_ID}"
  NAMESPACE_NAME="metrics-ns-${TEST_RUN_ID}"
  QUEUE_NAME="orders-${TEST_RUN_ID}"
}

build_metrics_payload() {
  APP_SERVICE="${APP_SERVICE}" \
  DB_SERVICE="${DB_SERVICE}" \
  QUEUE_SERVICE="${QUEUE_SERVICE}" \
  AI_SERVICE="${AI_SERVICE}" \
  HOST_NAME="${HOST_NAME}" \
  POD_NAME="${POD_NAME}" \
  CONTAINER_NAME="${CONTAINER_NAME}" \
  NODE_NAME="${NODE_NAME}" \
  NAMESPACE_NAME="${NAMESPACE_NAME}" \
  QUEUE_NAME="${QUEUE_NAME}" \
  ENVIRONMENT="${ENVIRONMENT}" \
  AI_MODEL_PRIMARY="${AI_MODEL_PRIMARY}" \
  AI_MODEL_SECONDARY="${AI_MODEL_SECONDARY}" \
  "${PYTHON_BIN}" - <<'PY'
import json
import os
import time

base_ts = int(time.time() * 1_000_000_000)
tick = 0

def next_ts():
    global tick
    tick += 1
    return base_ts + tick * 1_000_000

def sval(value):
    return {"stringValue": str(value)}

def ival(value):
    return {"intValue": str(int(value))}

def fval(value):
    return {"doubleValue": float(value)}

def bval(value):
    return {"boolValue": bool(value)}

def kv(key, value):
    return {"key": key, "value": value}

def attrs(pairs):
    return [kv(key, value) for key, value in pairs]

def point(value, point_attrs=None, integer=False):
    body = {"timeUnixNano": str(next_ts())}
    if point_attrs:
        body["attributes"] = point_attrs
    if integer:
        body["asInt"] = str(int(value))
    else:
        body["asDouble"] = float(value)
    return body

def gauge(name, value, point_attrs=None, unit="", description="", integer=False):
    return {
        "name": name,
        "unit": unit,
        "description": description,
        "gauge": {
            "dataPoints": [point(value, point_attrs, integer=integer)]
        },
    }

def summation(name, value, point_attrs=None, unit="", description="", monotonic=True, temporality=2, integer=False):
    return {
        "name": name,
        "unit": unit,
        "description": description,
        "sum": {
            "aggregationTemporality": temporality,
            "isMonotonic": monotonic,
            "dataPoints": [point(value, point_attrs, integer=integer)],
        },
    }

def histogram(name, count, total, bounds, bucket_counts, point_attrs=None, unit="", description="", temporality=2):
    return {
        "name": name,
        "unit": unit,
        "description": description,
        "histogram": {
            "aggregationTemporality": temporality,
            "dataPoints": [{
                "timeUnixNano": str(next_ts()),
                "count": int(count),
                "sum": float(total),
                "explicitBounds": [float(bound) for bound in bounds],
                "bucketCounts": [int(bucket) for bucket in bucket_counts],
                "attributes": point_attrs or [],
            }],
        },
    }

def resource_attrs(service):
    return attrs([
        ("service.name", sval(service)),
        ("deployment.environment", sval(os.environ["ENVIRONMENT"])),
        ("host.name", sval(os.environ["HOST_NAME"])),
        ("k8s.namespace.name", sval(os.environ["NAMESPACE_NAME"])),
        ("k8s.pod.name", sval(os.environ["POD_NAME"])),
        ("container.name", sval(os.environ["CONTAINER_NAME"])),
        ("k8s.node.name", sval(os.environ["NODE_NAME"])),
        ("telemetry.sdk.language", sval("python")),
        ("telemetry.sdk.version", sval("1.30.0")),
    ])

app_metrics = [
    histogram(
        "http.server.request.duration",
        120,
        5400.0,
        [10, 25, 50, 100, 250, 500],
        [10, 25, 40, 25, 15, 5, 0],
        attrs([
            ("http.request.method", sval("GET")),
            ("http.route", sval("/checkout")),
            ("http.response.status_code", ival(200)),
        ]),
        unit="ms",
        description="HTTP server duration",
    ),
    gauge(
        "http.server.active_requests",
        7,
        attrs([
            ("http.request.method", sval("GET")),
            ("http.route", sval("/checkout")),
        ]),
        unit="{request}",
        description="Active HTTP requests",
        integer=True,
    ),
    histogram(
        "http.server.request.body.size",
        60,
        32768.0,
        [128, 256, 512, 1024],
        [8, 15, 20, 12, 5],
        attrs([("http.route", sval("/checkout"))]),
        unit="By",
    ),
    histogram(
        "http.server.response.body.size",
        60,
        65536.0,
        [128, 256, 512, 1024],
        [6, 16, 18, 14, 6],
        attrs([("http.route", sval("/checkout"))]),
        unit="By",
    ),
    histogram(
        "http.client.request.duration",
        45,
        1480.0,
        [5, 10, 25, 50, 100],
        [6, 10, 12, 10, 5, 2],
        attrs([
            ("http.request.method", sval("POST")),
            ("url.full", sval("https://payments.internal/pay")),
            ("http.response.status_code", ival(200)),
        ]),
        unit="ms",
    ),
    histogram(
        "dns.lookup.duration",
        45,
        110.0,
        [1, 2, 5, 10],
        [5, 12, 15, 9, 4],
        attrs([("server.address", sval("payments.internal"))]),
        unit="ms",
    ),
    histogram(
        "tls.connect.duration",
        45,
        180.0,
        [1, 2, 5, 10],
        [4, 10, 17, 10, 4],
        attrs([("server.address", sval("payments.internal"))]),
        unit="ms",
        temporality=1,
    ),
    histogram(
        "rpc.server.duration",
        90,
        2800.0,
        [5, 10, 25, 50, 100, 250],
        [10, 20, 25, 20, 10, 4, 1],
        attrs([
            ("rpc.system", sval("grpc")),
            ("rpc.service", sval("payments.Checkout")),
            ("rpc.method", sval("Charge")),
            ("rpc.grpc.status_code", ival(0)),
        ]),
        unit="ms",
    ),
    histogram(
        "messaging.client.operation.duration",
        35,
        520.0,
        [2, 5, 10, 25, 50],
        [4, 8, 10, 8, 4, 1],
        attrs([
            ("messaging.system", sval("kafka")),
            ("messaging.destination.name", sval(os.environ["QUEUE_NAME"])),
            ("messaging.operation.name", sval("publish")),
        ]),
        unit="ms",
    ),
    summation(
        "process.cpu.time",
        12.8,
        attrs([("process.cpu.state", sval("user"))]),
        unit="s",
        monotonic=True,
        temporality=2,
    ),
    gauge("process.memory.usage", 153600000, unit="By"),
    gauge("process.memory.virtual", 412300000, unit="By"),
    gauge("process.open_file_descriptor.count", 83, unit="{fd}", integer=True),
    gauge("process.uptime", 7200, unit="s", integer=True),
    gauge("system.cpu.utilization", 0.63, attrs([("system.cpu.state", sval("user"))]), unit="1"),
    gauge("system.cpu.usage", 432.0, attrs([("system.cpu.state", sval("user"))]), unit="s"),
    gauge("process.cpu.usage", 0.41, unit="1"),
    gauge("system.memory.utilization", 0.74, attrs([("system.memory.state", sval("used"))]), unit="1"),
    gauge("system.disk.utilization", 0.58, attrs([("system.filesystem.mountpoint", sval("/var/lib"))]), unit="1"),
    gauge("system.network.utilization", 0.27, attrs([("system.network.io.direction", sval("transmit"))]), unit="1"),
    gauge("db.connection.pool.utilization", 0.46, unit="1"),
    gauge("hikaricp.connections.active", 9, integer=True),
    gauge("hikaricp.connections.max", 32, integer=True),
    gauge("jdbc.connections.active", 5, integer=True),
    gauge("jdbc.connections.max", 20, integer=True),
    summation("system.cpu.time", 910.0, attrs([("system.cpu.state", sval("user"))]), unit="s"),
    gauge("system.memory.usage", 810000000, attrs([("system.memory.state", sval("used"))]), unit="By"),
    gauge("system.paging.usage", 12000000, attrs([("system.memory.state", sval("used"))]), unit="By"),
    gauge("system.disk.io", 245760, attrs([("system.disk.direction", sval("read"))]), unit="By"),
    gauge("system.disk.operations", 320, attrs([("system.disk.direction", sval("read"))]), unit="{operation}", integer=True),
    gauge("system.disk.io_time", 92, attrs([("system.disk.direction", sval("read"))]), unit="ms"),
    gauge("system.filesystem.usage", 643825664, attrs([("system.filesystem.mountpoint", sval("/var/lib"))]), unit="By"),
    gauge("system.filesystem.utilization", 0.48, attrs([("system.filesystem.mountpoint", sval("/var/lib"))]), unit="1"),
    gauge("system.network.io", 524288, attrs([("system.network.io.direction", sval("receive"))]), unit="By"),
    gauge("system.network.packets", 1200, attrs([("system.network.io.direction", sval("receive"))]), unit="{packet}", integer=True),
    gauge("system.network.errors", 3, attrs([("system.network.state", sval("receive"))]), unit="{error}", integer=True),
    gauge("system.network.dropped", 2, attrs([("system.network.state", sval("receive"))]), unit="{packet}", integer=True),
    gauge("system.cpu.load_average.1m", 1.2, unit="1"),
    gauge("system.process.count", 148, attrs([("process.status", sval("running"))]), unit="{process}", integer=True),
    gauge("system.network.connections", 64, attrs([("network.state", sval("established"))]), unit="{connection}", integer=True),
    gauge("jvm.memory.used", 268435456, attrs([("jvm.memory.type", sval("heap")), ("jvm.memory.pool.name", sval("old-gen"))]), unit="By"),
    gauge("jvm.memory.committed", 536870912, attrs([("jvm.memory.type", sval("heap")), ("jvm.memory.pool.name", sval("old-gen"))]), unit="By"),
    gauge("jvm.memory.limit", 1073741824, attrs([("jvm.memory.type", sval("heap")), ("jvm.memory.pool.name", sval("old-gen"))]), unit="By"),
    histogram(
        "jvm.gc.duration",
        18,
        480.0,
        [5, 10, 25, 50, 100],
        [2, 4, 5, 4, 2, 1],
        attrs([("jvm.gc.name", sval("G1 Young Generation")), ("jvm.gc.action", sval("end of minor GC"))]),
        unit="ms",
        temporality=1,
    ),
    gauge("jvm.thread.count", 122, attrs([("jvm.thread.daemon", sval("true"))]), unit="{thread}", integer=True),
    gauge("jvm.class.loaded", 8400, unit="{class}", integer=True),
    gauge("jvm.class.count", 9100, unit="{class}", integer=True),
    summation("jvm.cpu.time", 245.0, unit="s"),
    gauge("jvm.cpu.recent_utilization", 0.39, unit="1"),
    gauge("jvm.buffer.memory.usage", 1048576, attrs([("jvm.buffer.pool.name", sval("direct"))]), unit="By"),
    gauge("jvm.buffer.count", 12, attrs([("jvm.buffer.pool.name", sval("direct"))]), unit="{buffer}", integer=True),
    summation("container.cpu.time", 133.0, attrs([("container.name", sval(os.environ["CONTAINER_NAME"]))]), unit="s"),
    summation("container.cpu.throttling_data.throttled_time", 14.0, attrs([("container.name", sval(os.environ["CONTAINER_NAME"]))]), unit="s"),
    gauge("container.memory.usage", 251658240, attrs([("container.name", sval(os.environ["CONTAINER_NAME"]))]), unit="By"),
    summation("container.memory.oom_kill_count", 1, attrs([("container.name", sval(os.environ["CONTAINER_NAME"]))]), unit="{event}", integer=True),
    summation("k8s.container.restarts", 2, attrs([("container.name", sval(os.environ["CONTAINER_NAME"])), ("k8s.pod.name", sval(os.environ["POD_NAME"]))]), unit="{restart}", integer=True),
    gauge("k8s.node.allocatable.cpu", 8, attrs([("k8s.node.name", sval(os.environ["NODE_NAME"]))]), unit="{cpu}", integer=True),
    gauge("k8s.node.allocatable.memory", 17179869184, attrs([("k8s.node.name", sval(os.environ["NODE_NAME"]))]), unit="By"),
    gauge("k8s.pod.phase", 1, attrs([("k8s.pod.phase", sval("Running")), ("k8s.pod.name", sval(os.environ["POD_NAME"]))]), integer=True),
    gauge("k8s.replicaset.desired", 4, attrs([("k8s.replicaset.name", sval("checkout-rs"))]), integer=True),
    gauge("k8s.replicaset.available", 3, attrs([("k8s.replicaset.name", sval("checkout-rs"))]), integer=True),
    gauge("k8s.volume.capacity", 21474836480, attrs([("k8s.volume.name", sval("checkout-pv"))]), unit="By"),
    gauge("k8s.volume.inodes", 1048576, attrs([("k8s.volume.name", sval("checkout-pv"))]), integer=True),
]

db_metrics = [
    histogram(
        "mongodb.driver.commands",
        80,
        2400.0,
        [5, 10, 25, 50, 100],
        [10, 16, 24, 18, 10, 2],
        attrs([
            ("db.system", sval("mongodb")),
            ("db.name", sval("catalog")),
            ("db.mongodb.collection", sval("orders")),
            ("collection", sval("orders")),
        ]),
        unit="ms",
    ),
    histogram(
        "hikaricp.connections.usage",
        55,
        820.0,
        [2, 5, 10, 25, 50],
        [8, 14, 15, 12, 5, 1],
        attrs([
            ("db.system", sval("mysql")),
            ("pool", sval("orders-pool")),
            ("db.sql.table", sval("payments")),
        ]),
        unit="ms",
    ),
    summation("db.cache.hits", 320, attrs([("db.system", sval("mysql"))]), integer=True),
    summation("db.cache.misses", 45, attrs([("db.system", sval("mysql"))]), integer=True),
    gauge("db.replication.lag.ms", 18, attrs([("db.system", sval("mysql"))]), unit="ms"),
    summation("db.client.errors", 3, attrs([("db.system", sval("mysql"))]), integer=True),
    gauge("db.client.connection.count", 24, attrs([("db.client.connection.state", sval("idle")), ("db.client.connection.pool.name", sval("orders-pool"))]), integer=True),
    histogram("db.client.connection.wait_time", 22, 160.0, [1, 2, 5, 10], [4, 7, 6, 4, 1], attrs([("db.client.connection.pool.name", sval("orders-pool"))]), unit="ms"),
    gauge("db.client.connection.pending_requests", 4, attrs([("db.client.connection.pool.name", sval("orders-pool"))]), integer=True),
    summation("db.client.connection.timeouts", 1, attrs([("db.client.connection.pool.name", sval("orders-pool"))]), integer=True),
    histogram(
        "db.client.operation.duration",
        70,
        1320.0,
        [2, 5, 10, 25, 50],
        [10, 15, 18, 17, 8, 2],
        attrs([
            ("db.system", sval("mysql")),
            ("db.name", sval("orders")),
            ("db.operation", sval("SELECT")),
            ("db.sql.table", sval("payments")),
            ("db.operation.name", sval("select")),
        ]),
        unit="ms",
        temporality=1,
    ),
    summation("redis.keyspace.hits", 1500, attrs([("redis.db", sval("0"))]), integer=True),
    summation("redis.keyspace.misses", 80, attrs([("redis.db", sval("0"))]), integer=True),
    gauge("redis.clients.connected", 42, integer=True),
    gauge("redis.memory.used", 73400320, unit="By"),
    gauge("redis.memory.fragmentation_ratio", 1.28, unit="1"),
    summation("redis.commands.processed", 2400, integer=True),
    summation("redis.keys.evicted", 12, integer=True),
    gauge("redis.db.keys", 9100, attrs([("redis.db", sval("0"))]), integer=True),
    gauge("redis.db.expires", 680, attrs([("redis.db", sval("0"))]), integer=True),
    gauge("redis.replication.offset", 932000, integer=True),
    gauge("redis.replication.backlog_first_byte_offset", 931400, integer=True),
]

queue_metrics = [
    gauge(
        "kafka.consumer.lag",
        42,
        attrs([
            ("messaging.system", sval("kafka")),
            ("messaging.destination.name", sval(os.environ["QUEUE_NAME"])),
            ("messaging.kafka.consumer.group", sval("checkout-workers")),
            ("messaging.kafka.destination.partition", ival(0)),
        ]),
        integer=True,
    ),
    summation(
        "kafka.producer.message.count",
        620,
        attrs([
            ("messaging.system", sval("kafka")),
            ("messaging.destination.name", sval(os.environ["QUEUE_NAME"])),
        ]),
        integer=True,
        temporality=1,
    ),
    summation(
        "kafka.consumer.message.count",
        580,
        attrs([
            ("messaging.system", sval("kafka")),
            ("messaging.destination.name", sval(os.environ["QUEUE_NAME"])),
            ("messaging.kafka.consumer.group", sval("checkout-workers")),
        ]),
        integer=True,
    ),
    gauge(
        "queue.depth",
        26,
        attrs([
            ("messaging.system", sval("kafka")),
            ("messaging.destination.name", sval(os.environ["QUEUE_NAME"])),
        ]),
        integer=True,
    ),
    gauge(
        "messaging.kafka.consumer.lag",
        41,
        attrs([
            ("messaging.system", sval("kafka")),
            ("messaging.destination.name", sval(os.environ["QUEUE_NAME"])),
            ("messaging.kafka.consumer.group", sval("checkout-workers")),
        ]),
        integer=True,
    ),
    summation(
        "messaging.client.published.messages",
        615,
        attrs([
            ("messaging.system", sval("kafka")),
            ("messaging.destination.name", sval(os.environ["QUEUE_NAME"])),
        ]),
        integer=True,
    ),
    summation(
        "messaging.client.consumed.messages",
        579,
        attrs([
            ("messaging.system", sval("kafka")),
            ("messaging.destination.name", sval(os.environ["QUEUE_NAME"])),
            ("messaging.kafka.consumer.group", sval("checkout-workers")),
        ]),
        integer=True,
        temporality=1,
    ),
    histogram(
        "messaging.client.operation.duration",
        30,
        340.0,
        [2, 5, 10, 25],
        [4, 8, 10, 6, 2],
        attrs([
            ("messaging.system", sval("kafka")),
            ("messaging.destination.name", sval(os.environ["QUEUE_NAME"])),
            ("messaging.operation.name", sval("process")),
        ]),
        unit="ms",
    ),
    gauge(
        "messaging.kafka.consumer.offset",
        9081,
        attrs([
            ("messaging.system", sval("kafka")),
            ("messaging.destination.name", sval(os.environ["QUEUE_NAME"])),
            ("messaging.kafka.consumer.group", sval("checkout-workers")),
        ]),
        integer=True,
    ),
]

def ai_attrs(model, provider, operation, duration_ms, prompt_tokens, completion_tokens, cost, cache_hit, timeout, retry_count, pii_detected, guardrail_blocked, content_policy, error_flag, pii_categories):
    return attrs([
        ("gen.ai.request.model", sval(model)),
        ("server.address", sval(provider)),
        ("gen.ai.operation.name", sval(operation)),
        ("duration_ms", fval(duration_ms)),
        ("gen.ai.usage.input_tokens", ival(prompt_tokens)),
        ("gen.ai.usage.output_tokens", ival(completion_tokens)),
        ("gen.ai.usage.cache_read_input_tokens", ival(128)),
        ("ai.cost_usd", fval(cost)),
        ("ai.timeout", ival(timeout)),
        ("ai.cache_hit", ival(cache_hit)),
        ("ai.retry_count", ival(retry_count)),
        ("ai.security.pii_detected", ival(pii_detected)),
        ("ai.security.guardrail_blocked", ival(guardrail_blocked)),
        ("ai.security.content_policy", ival(content_policy)),
        ("ai.security.pii.categories", sval(pii_categories)),
        ("error", bval(error_flag)),
    ])

ai_metrics = [
    histogram(
        "gen_ai.client.operation.duration",
        12,
        9600.0,
        [100, 250, 500, 1000, 2000],
        [1, 2, 3, 4, 2, 0],
        ai_attrs(os.environ["AI_MODEL_PRIMARY"], "openai", "chat", 800, 1200, 320, 0.024, 1, 0, 1, 1, 0, 1, False, "email,phone"),
        unit="ms",
    ),
    summation(
        "gen_ai.client.operation.count",
        12,
        ai_attrs(os.environ["AI_MODEL_PRIMARY"], "openai", "chat", 840, 1190, 300, 0.022, 0, 0, 0, 0, 0, 0, False, ""),
        integer=True,
    ),
    summation(
        "gen_ai.client.token.usage",
        1820,
        ai_attrs(os.environ["AI_MODEL_PRIMARY"], "openai", "chat", 910, 1220, 600, 0.031, 1, 0, 1, 1, 0, 1, False, "email"),
        integer=True,
        temporality=1,
    ),
    summation(
        "gen_ai.client.operation.cost",
        0.044,
        ai_attrs(os.environ["AI_MODEL_SECONDARY"], "openai", "generate_content", 1650, 2200, 540, 0.044, 0, 1, 2, 1, 1, 1, True, "credit_card"),
        monotonic=False,
        temporality=1,
    ),
]

resource_metrics = [
    {
        "resource": {"attributes": resource_attrs(os.environ["APP_SERVICE"])},
        "scopeMetrics": [{"scope": {"name": "codex.metrics.http-apm", "version": "1.0.0"}, "metrics": app_metrics}],
    },
    {
        "resource": {"attributes": resource_attrs(os.environ["DB_SERVICE"])},
        "scopeMetrics": [{"scope": {"name": "codex.metrics.database", "version": "1.0.0"}, "metrics": db_metrics}],
    },
    {
        "resource": {"attributes": resource_attrs(os.environ["QUEUE_SERVICE"])},
        "scopeMetrics": [{"scope": {"name": "codex.metrics.queue", "version": "1.0.0"}, "metrics": queue_metrics}],
    },
    {
        "resource": {"attributes": resource_attrs(os.environ["AI_SERVICE"])},
        "scopeMetrics": [{"scope": {"name": "codex.metrics.ai", "version": "1.0.0"}, "metrics": ai_metrics}],
    },
]

print(json.dumps({"resourceMetrics": resource_metrics}))
PY
}

verify_metrics_ingestion() {
  print_section "Metrics Ingestion Verification"

  if ! clickhouse_ready; then
    echo "Skipping ClickHouse verification because ${CLICKHOUSE_CONTAINER} is not reachable."
    return
  fi

  echo "Metric types inserted:"
  clickhouse_query "
    SELECT metric_type, count()
    FROM observability.metrics
    WHERE team_id = '${TEAM_ID}'
      AND service IN ('${APP_SERVICE}', '${DB_SERVICE}', '${QUEUE_SERVICE}', '${AI_SERVICE}')
    GROUP BY metric_type
    ORDER BY metric_type
    FORMAT Pretty
  "

  echo ""
  echo "Metric category counts:"
  clickhouse_query "
    SELECT
      countIf(metric_name LIKE 'http.%') AS http_metrics,
      countIf(metric_name LIKE 'rpc.%' OR metric_name LIKE 'process.%' OR metric_name LIKE 'messaging.client.%') AS apm_metrics,
      countIf(metric_name LIKE 'system.%' OR metric_name LIKE 'jvm.%' OR metric_name LIKE 'container.%' OR metric_name LIKE 'k8s.%' OR metric_name LIKE 'db.connection.pool.%' OR metric_name LIKE 'hikaricp.%' OR metric_name LIKE 'jdbc.%') AS infra_metrics,
      countIf(metric_name LIKE 'db.%' OR metric_name LIKE 'redis.%' OR metric_name LIKE 'mongodb.%') AS database_metrics,
      countIf(metric_name LIKE 'kafka.%' OR metric_name LIKE 'queue.%' OR metric_name LIKE 'messaging.kafka.%') AS queue_metrics,
      countIf(metric_name LIKE 'gen_ai.%') AS ai_metrics
    FROM observability.metrics
    WHERE team_id = '${TEAM_ID}'
      AND service IN ('${APP_SERVICE}', '${DB_SERVICE}', '${QUEUE_SERVICE}', '${AI_SERVICE}')
    FORMAT Pretty
  "

  echo ""
  echo "Recent metrics:"
  clickhouse_query "
    SELECT metric_name, metric_type, service, value, hist_count, timestamp
    FROM observability.metrics
    WHERE team_id = '${TEAM_ID}'
      AND service IN ('${APP_SERVICE}', '${DB_SERVICE}', '${QUEUE_SERVICE}', '${AI_SERVICE}')
    ORDER BY timestamp DESC
    LIMIT 20
    FORMAT Pretty
  "
}

run_metrics_api_suite() {
  print_section "Metrics API Suite"
  api_suite_reset

  local q="?start=${START_MS}&end=${END_MS}"
  local ai_hist_q="?start=${START_MS}&end=${END_MS}&modelName=${AI_MODEL_PRIMARY}"

  local checks=(
    "HTTP request rate|/http/request-rate${q}|non-empty"
    "HTTP request duration|/http/request-duration${q}|success"
    "HTTP active requests|/http/active-requests${q}|non-empty"
    "HTTP request body size|/http/request-body-size${q}|success"
    "HTTP response body size|/http/response-body-size${q}|success"
    "HTTP client duration|/http/client-duration${q}|success"
    "HTTP DNS duration|/http/dns-duration${q}|success"
    "HTTP TLS duration|/http/tls-duration${q}|success"
    "APM RPC duration|/apm/rpc-duration${q}|success"
    "APM RPC request rate|/apm/rpc-request-rate${q}|success"
    "APM messaging publish duration|/apm/messaging-publish-duration${q}|success"
    "APM process CPU|/apm/process-cpu${q}|success"
    "APM process memory|/apm/process-memory${q}|success"
    "APM open FDs|/apm/open-fds${q}|success"
    "APM uptime|/apm/uptime${q}|success"
    "DB query by table|/saturation/database/query-by-table${q}|non-empty"
    "DB avg latency|/saturation/database/avg-latency${q}|success"
    "DB latency summary|/saturation/database/latency-summary${q}|success"
    "DB systems|/saturation/database/systems${q}|success"
    "DB top tables|/saturation/database/top-tables${q}|success"
    "DB connection count|/saturation/database/connection-count${q}|success"
    "DB connection wait time|/saturation/database/connection-wait-time${q}|success"
    "DB connection pending|/saturation/database/connection-pending${q}|success"
    "DB connection timeouts|/saturation/database/connection-timeouts${q}|success"
    "DB query duration|/saturation/database/query-duration${q}|success"
    "Redis cache hit rate|/saturation/redis/cache-hit-rate${q}|success"
    "Redis clients|/saturation/redis/clients${q}|success"
    "Redis memory|/saturation/redis/memory${q}|success"
    "Redis memory fragmentation|/saturation/redis/memory-fragmentation${q}|success"
    "Redis commands|/saturation/redis/commands${q}|success"
    "Redis evictions|/saturation/redis/evictions${q}|success"
    "Redis keyspace|/saturation/redis/keyspace${q}|success"
    "Redis key expiries|/saturation/redis/key-expiries${q}|success"
    "Redis replication lag|/saturation/redis/replication-lag${q}|success"
    "Kafka queue lag|/saturation/kafka/queue-lag${q}|success"
    "Kafka production rate|/saturation/kafka/production-rate${q}|success"
    "Kafka consumption rate|/saturation/kafka/consumption-rate${q}|success"
    "Queue consumer lag|/saturation/queue/consumer-lag${q}|success"
    "Queue topic lag|/saturation/queue/topic-lag${q}|success"
    "Queue top queues|/saturation/queue/top-queues${q}|success"
    "Queue consumer lag detail|/saturation/queue/consumer-lag-detail${q}|success"
    "Queue message rates|/saturation/queue/message-rates${q}|success"
    "Queue operation duration|/saturation/queue/operation-duration${q}|success"
    "Queue offset commit rate|/saturation/queue/offset-commit-rate${q}|success"
    "Resource util avg CPU|/infrastructure/resource-utilisation/avg-cpu${q}|success"
    "Resource util avg memory|/infrastructure/resource-utilisation/avg-memory${q}|success"
    "Resource util avg network|/infrastructure/resource-utilisation/avg-network${q}|success"
    "Resource util avg conn pool|/infrastructure/resource-utilisation/avg-conn-pool${q}|success"
    "Resource util CPU percentage|/infrastructure/resource-utilisation/cpu-usage-percentage${q}|success"
    "Resource util memory percentage|/infrastructure/resource-utilisation/memory-usage-percentage${q}|success"
    "Resource util by service|/infrastructure/resource-utilisation/by-service${q}|success"
    "Resource util by instance|/infrastructure/resource-utilisation/by-instance${q}|success"
    "Infrastructure CPU time|/infrastructure/cpu-time${q}|success"
    "Infrastructure memory usage|/infrastructure/memory-usage${q}|success"
    "Infrastructure swap usage|/infrastructure/swap-usage${q}|success"
    "Infrastructure disk IO|/infrastructure/disk-io${q}|success"
    "Infrastructure disk operations|/infrastructure/disk-operations${q}|success"
    "Infrastructure disk IO time|/infrastructure/disk-io-time${q}|success"
    "Infrastructure filesystem usage|/infrastructure/filesystem-usage${q}|success"
    "Infrastructure filesystem utilization|/infrastructure/filesystem-utilization${q}|success"
    "Infrastructure network IO|/infrastructure/network-io${q}|success"
    "Infrastructure network packets|/infrastructure/network-packets${q}|success"
    "Infrastructure network errors|/infrastructure/network-errors${q}|success"
    "Infrastructure network dropped|/infrastructure/network-dropped${q}|success"
    "Infrastructure load average|/infrastructure/load-average${q}|success"
    "Infrastructure process count|/infrastructure/process-count${q}|success"
    "Infrastructure network connections|/infrastructure/network-connections${q}|success"
    "JVM memory|/infrastructure/jvm/memory${q}|success"
    "JVM GC duration|/infrastructure/jvm/gc-duration${q}|success"
    "JVM GC collections|/infrastructure/jvm/gc-collections${q}|success"
    "JVM threads|/infrastructure/jvm/threads${q}|success"
    "JVM classes|/infrastructure/jvm/classes${q}|success"
    "JVM CPU|/infrastructure/jvm/cpu${q}|success"
    "JVM buffers|/infrastructure/jvm/buffers${q}|success"
    "Kubernetes container CPU|/infrastructure/kubernetes/container-cpu${q}|success"
    "Kubernetes CPU throttling|/infrastructure/kubernetes/cpu-throttling${q}|success"
    "Kubernetes container memory|/infrastructure/kubernetes/container-memory${q}|success"
    "Kubernetes OOM kills|/infrastructure/kubernetes/oom-kills${q}|success"
    "Kubernetes pod restarts|/infrastructure/kubernetes/pod-restarts${q}|success"
    "Kubernetes node allocatable|/infrastructure/kubernetes/node-allocatable${q}|success"
    "Kubernetes pod phases|/infrastructure/kubernetes/pod-phases${q}|success"
    "Kubernetes replica status|/infrastructure/kubernetes/replica-status${q}|success"
    "Kubernetes volume usage|/infrastructure/kubernetes/volume-usage${q}|success"
    "AI summary|/ai/summary${q}|success"
    "AI models|/ai/models${q}|non-empty"
    "AI performance metrics|/ai/performance/metrics${q}|success"
    "AI performance timeseries|/ai/performance/timeseries${q}|success"
    "AI latency histogram|/ai/performance/latency-histogram${ai_hist_q}|success"
    "AI cost metrics|/ai/cost/metrics${q}|success"
    "AI cost timeseries|/ai/cost/timeseries${q}|success"
    "AI token breakdown|/ai/cost/token-breakdown${q}|success"
    "AI security metrics|/ai/security/metrics${q}|success"
    "AI security timeseries|/ai/security/timeseries${q}|success"
    "AI PII categories|/ai/security/pii-categories${q}|success"
  )

  local entry
  for entry in "${checks[@]}"; do
    IFS='|' read -r label path expectation <<<"${entry}"
    run_api_check "${label}" "${path}" "${expectation}"
  done

  print_api_summary "Metrics API Results"
}

main() {
  print_section "OTLP Metrics Comprehensive Test"
  bootstrap_test_identity "metrics"
  init_metric_context

  echo "Sending comprehensive OTLP metrics payload..."
  otlp_post_json "/v1/metrics" "$(build_metrics_payload)"
  if [[ "${HTTP_STATUS}" != "200" ]]; then
    echo "Metrics ingestion request failed"
    echo "HTTP status: ${HTTP_STATUS}"
    echo "${HTTP_BODY}"
    exit 1
  fi
  echo "Metrics payload accepted"

  wait_for_async_ingest
  verify_metrics_ingestion
  run_metrics_api_suite

  if [[ "${API_FAIL}" -gt 0 ]]; then
    exit 1
  fi
}

main "$@"
