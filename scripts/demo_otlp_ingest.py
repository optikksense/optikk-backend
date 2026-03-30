#!/usr/bin/env python3
"""
Seed Optikk with realistic OTLP telemetry over gRPC for demos and screenshots.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import grpc
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2, logs_service_pb2_grpc
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2, metrics_service_pb2_grpc
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2, trace_service_pb2_grpc
from opentelemetry.proto.common.v1 import common_pb2
from opentelemetry.proto.logs.v1 import logs_pb2
from opentelemetry.proto.metrics.v1 import metrics_pb2
from opentelemetry.proto.resource.v1 import resource_pb2
from opentelemetry.proto.trace.v1 import trace_pb2

STATUS_UNSET = 0
STATUS_OK = 1
STATUS_ERROR = 2

TEMPORALITY_UNSPECIFIED = 0
TEMPORALITY_DELTA = 1
TEMPORALITY_CUMULATIVE = 2

SEVERITY_TRACE = 1
SEVERITY_DEBUG = 5
SEVERITY_INFO = 9
SEVERITY_WARN = 13
SEVERITY_ERROR = 17

ENVIRONMENT = "demo"
STATE_FILE_DEFAULT = "/tmp/optikk-demo-team.json"
BOOTSTRAP_COLOR = "#2563EB"
INSTRUMENTATION_SCOPE = "optikk-demo-generator"
INSTRUMENTATION_VERSION = "1.0"
AI_MODELS = [
    "gpt-4.1",
    "gpt-4o-mini",
    "claude-3-7-sonnet",
]
KAFKA_BROKER = "kafka.demo.internal:9092"
REDIS_ADDR = "redis.demo.internal:6379"
POSTGRES_ADDR = "postgres.demo.internal:5432"
EXTERNAL_HOSTS = [
    "api.stripe.com",
    "api.github.com",
    "sqs.us-east-1.amazonaws.com",
    "api.sendgrid.com",
    "api.twilio.com",
    "fcm.googleapis.com",
]


@dataclass(frozen=True)
class ResourceSpec:
    service_name: str
    host_name: str
    pod_name: str
    container_name: str
    namespace: str = "optikk-demo"

    def attributes(self) -> dict[str, Any]:
        return {
            "service.name": self.service_name,
            "deployment.environment": ENVIRONMENT,
            "host.name": self.host_name,
            "k8s.namespace.name": self.namespace,
            "k8s.pod.name": self.pod_name,
            "container.name": self.container_name,
        }


@dataclass
class TeamState:
    team_id: int
    api_key: str
    org_name: str
    team_name: str
    created_at: str
    user_id: int = 0
    user_email: str = ""
    user_password: str = ""
    user_name: str = ""


@dataclass(frozen=True)
class ServiceProfile:
    base_cpu_util: float
    base_memory_util: float
    base_disk_util: float
    base_network_util: float
    memory_total: int
    disk_total: int
    network_capacity_bytes: int
    fd_base: int
    connection_base: int
    db_pool_max: int = 0
    redis_memory_floor: int = 0
    redis_keys_floor: int = 0
    kafka_partitions: int = 0


@dataclass
class ServiceRuntimeState:
    cpu_util: float
    mem_util: float
    disk_util: float
    net_util: float
    active_requests: int
    request_rate: float
    memory_used: int
    disk_free: int
    rx_bytes_per_sec: int
    tx_bytes_per_sec: int
    connections: int
    fd_count: int
    db_active: int = 0
    db_pending: int = 0
    db_wait_ms: float = 2.0
    db_use_ms: float = 8.0
    redis_clients: int = 0
    redis_memory_used: int = 0
    redis_fragmentation_ratio: float = 1.08
    redis_keys: int = 0
    redis_expires: int = 0
    kafka_lag: int = 0
    kafka_lag_sum: int = 0
    kafka_assigned: int = 0
    kafka_process_ms: float = 18.0
    process_cpu_usage: float = 0.0
    restarts: int = 0


@dataclass
class ServiceActivity:
    http_requests: int = 0
    http_errors: int = 0
    total_http_duration_ms: float = 0.0
    db_calls: int = 0
    db_duration_ms: float = 0.0
    redis_calls: int = 0
    redis_misses: int = 0
    kafka_publishes: int = 0
    kafka_receives: int = 0
    kafka_processes: int = 0
    ai_requests: int = 0
    ai_input_tokens: int = 0
    ai_output_tokens: int = 0


@dataclass
class SpanEventSpec:
    name: str
    time_unix_nano: int
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class SpanSpec:
    resource: ResourceSpec
    trace_id: bytes
    span_id: bytes
    parent_span_id: bytes
    name: str
    kind: int
    start_unix_nano: int
    end_unix_nano: int
    attributes: dict[str, Any] = field(default_factory=dict)
    status_code: int = STATUS_OK
    status_message: str = ""
    events: list[SpanEventSpec] = field(default_factory=list)


@dataclass
class LogSpec:
    resource: ResourceSpec
    trace_id: bytes
    span_id: bytes
    timestamp_nano: int
    observed_timestamp_nano: int
    severity_text: str
    severity_number: int
    body: str
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class NumberMetricSpec:
    resource: ResourceSpec
    name: str
    value: float
    attributes: dict[str, Any]
    description: str = ""
    unit: str = ""
    monotonic: bool = False
    temporality: int = TEMPORALITY_CUMULATIVE
    as_int: bool = False


@dataclass
class HistogramMetricSpec:
    resource: ResourceSpec
    name: str
    count: int
    sum_value: float
    explicit_bounds: list[float]
    bucket_counts: list[int]
    attributes: dict[str, Any]
    description: str = ""
    unit: str = ""
    temporality: int = TEMPORALITY_CUMULATIVE


@dataclass
class ScenarioResult:
    name: str
    spans: list[SpanSpec]
    logs: list[LogSpec]
    metrics: list[NumberMetricSpec | HistogramMetricSpec]


SERVICE_RESOURCES: dict[str, ResourceSpec] = {
    "web-gateway": ResourceSpec("web-gateway", "gateway-01", "web-gateway-7f8d9", "web-gateway"),
    "checkout-api": ResourceSpec("checkout-api", "checkout-01", "checkout-api-6bc7d", "checkout-api"),
    "catalog-api": ResourceSpec("catalog-api", "catalog-01", "catalog-api-7cc95", "catalog-api"),
    "payment-api": ResourceSpec("payment-api", "payment-01", "payment-api-7744d", "payment-api"),
    "orders-worker": ResourceSpec("orders-worker", "orders-01", "orders-worker-54dfc", "orders-worker"),
    "recommendation-ai": ResourceSpec("recommendation-ai", "ai-01", "recommendation-ai-19dda", "recommendation-ai"),
    "auth-api": ResourceSpec("auth-api", "auth-01", "auth-api-44da1", "auth-api"),
    "inventory-api": ResourceSpec("inventory-api", "inventory-01", "inventory-api-2cd71", "inventory-api"),
    "notification-api": ResourceSpec("notification-api", "notify-01", "notification-api-6fa2b", "notification-api"),
    "search-api": ResourceSpec("search-api", "search-01", "search-api-88fb0", "search-api"),
}

SCENARIO_ORDER = (
    "healthy_checkout",
    "slow_db_checkout",
    "payment_failure",
    "order_worker_kafka_lag",
    "redis_pressure",
    "ai_support_chat",
)

SERVICE_PROFILES: dict[str, ServiceProfile] = {
    "web-gateway": ServiceProfile(
        base_cpu_util=0.22,
        base_memory_util=0.38,
        base_disk_util=0.28,
        base_network_util=0.20,
        memory_total=4_000_000_000,
        disk_total=2_500_000_000,
        network_capacity_bytes=12_000_000,
        fd_base=135,
        connection_base=42,
    ),
    "checkout-api": ServiceProfile(
        base_cpu_util=0.28,
        base_memory_util=0.44,
        base_disk_util=0.31,
        base_network_util=0.24,
        memory_total=5_000_000_000,
        disk_total=2_800_000_000,
        network_capacity_bytes=14_000_000,
        fd_base=148,
        connection_base=55,
        db_pool_max=32,
        redis_memory_floor=280_000_000,
        redis_keys_floor=11_200,
        kafka_partitions=6,
    ),
    "catalog-api": ServiceProfile(
        base_cpu_util=0.24,
        base_memory_util=0.41,
        base_disk_util=0.29,
        base_network_util=0.22,
        memory_total=4_500_000_000,
        disk_total=2_600_000_000,
        network_capacity_bytes=11_000_000,
        fd_base=142,
        connection_base=48,
        db_pool_max=32,
    ),
    "payment-api": ServiceProfile(
        base_cpu_util=0.26,
        base_memory_util=0.40,
        base_disk_util=0.30,
        base_network_util=0.23,
        memory_total=4_200_000_000,
        disk_total=2_400_000_000,
        network_capacity_bytes=10_000_000,
        fd_base=138,
        connection_base=44,
        db_pool_max=32,
        redis_memory_floor=160_000_000,
        redis_keys_floor=4_200,
    ),
    "orders-worker": ServiceProfile(
        base_cpu_util=0.20,
        base_memory_util=0.35,
        base_disk_util=0.27,
        base_network_util=0.18,
        memory_total=3_800_000_000,
        disk_total=2_800_000_000,
        network_capacity_bytes=9_000_000,
        fd_base=118,
        connection_base=34,
        db_pool_max=24,
        kafka_partitions=12,
    ),
    "recommendation-ai": ServiceProfile(
        base_cpu_util=0.32,
        base_memory_util=0.46,
        base_disk_util=0.26,
        base_network_util=0.19,
        memory_total=6_000_000_000,
        disk_total=2_400_000_000,
        network_capacity_bytes=8_000_000,
        fd_base=126,
        connection_base=28,
        kafka_partitions=2,
    ),
    "auth-api": ServiceProfile(
        base_cpu_util=0.18,
        base_memory_util=0.33,
        base_disk_util=0.24,
        base_network_util=0.17,
        memory_total=3_600_000_000,
        disk_total=2_200_000_000,
        network_capacity_bytes=8_500_000,
        fd_base=110,
        connection_base=38,
        db_pool_max=16,
        redis_memory_floor=120_000_000,
        redis_keys_floor=2_200,
    ),
    "inventory-api": ServiceProfile(
        base_cpu_util=0.24,
        base_memory_util=0.39,
        base_disk_util=0.28,
        base_network_util=0.20,
        memory_total=4_400_000_000,
        disk_total=2_900_000_000,
        network_capacity_bytes=10_500_000,
        fd_base=132,
        connection_base=40,
        db_pool_max=24,
        kafka_partitions=4,
    ),
    "notification-api": ServiceProfile(
        base_cpu_util=0.16,
        base_memory_util=0.31,
        base_disk_util=0.23,
        base_network_util=0.16,
        memory_total=3_200_000_000,
        disk_total=2_100_000_000,
        network_capacity_bytes=7_500_000,
        fd_base=102,
        connection_base=26,
        kafka_partitions=4,
    ),
    "search-api": ServiceProfile(
        base_cpu_util=0.27,
        base_memory_util=0.42,
        base_disk_util=0.27,
        base_network_util=0.22,
        memory_total=4_800_000_000,
        disk_total=2_700_000_000,
        network_capacity_bytes=11_500_000,
        fd_base=136,
        connection_base=46,
        db_pool_max=20,
        redis_memory_floor=210_000_000,
        redis_keys_floor=8_800,
    ),
}


class DemoOTLPGenerator:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.rng = random.Random(args.seed)
        self.loop_index = 0
        self.started_at_unix = time.time()
        self.counters: dict[str, float] = defaultdict(float)
        self.ai_conversation_turns = {
            "support-convo-001": 0,
            "support-convo-002": 0,
            "support-convo-003": 0,
        }
        self.service_states = self.initialize_service_states()
        self.reset_channel()

    def initialize_service_states(self) -> dict[str, ServiceRuntimeState]:
        states: dict[str, ServiceRuntimeState] = {}
        for service_name, profile in SERVICE_PROFILES.items():
            memory_used = int(profile.memory_total * profile.base_memory_util)
            states[service_name] = ServiceRuntimeState(
                cpu_util=profile.base_cpu_util,
                mem_util=profile.base_memory_util,
                disk_util=profile.base_disk_util,
                net_util=profile.base_network_util,
                active_requests=max(1, int(profile.connection_base * 0.12)),
                request_rate=0.5,
                memory_used=memory_used,
                disk_free=max(profile.disk_total - int(profile.disk_total * profile.base_disk_util), 0),
                rx_bytes_per_sec=int(profile.network_capacity_bytes * profile.base_network_util * 0.58),
                tx_bytes_per_sec=int(profile.network_capacity_bytes * profile.base_network_util * 0.49),
                connections=profile.connection_base,
                fd_count=profile.fd_base,
                db_active=max(2, profile.db_pool_max // 4) if profile.db_pool_max else 0,
                db_pending=0,
                db_wait_ms=2.4,
                db_use_ms=9.0,
                redis_clients=max(10, profile.connection_base // 2) if profile.redis_memory_floor else 0,
                redis_memory_used=profile.redis_memory_floor,
                redis_fragmentation_ratio=1.08,
                redis_keys=profile.redis_keys_floor,
                redis_expires=max(profile.redis_keys_floor // 4, 0),
                kafka_lag=6 if profile.kafka_partitions else 0,
                kafka_lag_sum=24 if profile.kafka_partitions else 0,
                kafka_assigned=profile.kafka_partitions,
                kafka_process_ms=22.0,
                process_cpu_usage=min(profile.base_cpu_util + 0.05, 0.92),
                restarts=1 if service_name == "orders-worker" else 0,
            )
        return states

    def reset_channel(self) -> None:
        self.channel = grpc.insecure_channel(
            self.args.backend_grpc,
            options=[
                ("grpc.keepalive_time_ms", 30_000),
                ("grpc.keepalive_timeout_ms", 10_000),
                ("grpc.keepalive_permit_without_calls", 1),
            ],
        )
        self.trace_stub = trace_service_pb2_grpc.TraceServiceStub(self.channel)
        self.logs_stub = logs_service_pb2_grpc.LogsServiceStub(self.channel)
        self.metrics_stub = metrics_service_pb2_grpc.MetricsServiceStub(self.channel)

    def bootstrap_team(self) -> TeamState:
        if self.args.api_key:
            return TeamState(
                team_id=0,
                api_key=self.args.api_key,
                org_name=self.args.org_name,
                team_name=self.args.team_name,
                created_at=iso_now(),
            )

        state_path = Path(self.args.state_file)
        if state_path.exists() and not self.args.reset_team:
            state = TeamState(**json.loads(state_path.read_text(encoding="utf-8")))
            if not state.user_email or not state.user_password:
                state = self.ensure_demo_user(state)
                self.write_state(state)
            return state

        team_state = self.create_team()
        team_state = self.ensure_demo_user(team_state)
        self.write_state(team_state)
        return team_state

    def write_state(self, state: TeamState) -> None:
        state_path = Path(self.args.state_file)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(
            json.dumps(
                {
                    "team_id": state.team_id,
                    "api_key": state.api_key,
                    "org_name": state.org_name,
                    "team_name": state.team_name,
                    "created_at": state.created_at,
                    "user_id": state.user_id,
                    "user_email": state.user_email,
                    "user_password": state.user_password,
                    "user_name": state.user_name,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    def create_team(self) -> TeamState:
        base_team_name = self.args.team_name
        base_slug = slugify(base_team_name)

        for attempt in range(2):
            team_name = base_team_name
            slug = base_slug
            if attempt == 1:
                suffix = time.strftime("%m%d%H%M")
                team_name = f"{base_team_name} {suffix}"
                slug = f"{base_slug}-{suffix}"

            payload = {
                "team_name": team_name,
                "org_name": self.args.org_name,
                "slug": slug,
                "description": "Synthetic OTLP demo telemetry team for screenshots and product storytelling.",
                "color": BOOTSTRAP_COLOR,
            }
            try:
                response = http_json(
                    f"{self.args.backend_http.rstrip('/')}/api/v1/teams",
                    payload,
                )
                if not response.get("success"):
                    raise RuntimeError(
                        f"team bootstrap failed: {response.get('error', {}).get('message', 'unknown error')}"
                    )
                data = response.get("data") or {}
                return TeamState(
                    team_id=int(data.get("id", 0)),
                    api_key=str(data["api_key"]),
                    org_name=str(data.get("org_name", self.args.org_name)),
                    team_name=str(data.get("name", team_name)),
                    created_at=str(data.get("created_at") or iso_now()),
                )
            except Exception as exc:
                if attempt == 0 and "already exists" in str(exc).lower():
                    continue
                raise

        raise RuntimeError("failed to bootstrap team after retry")

    def ensure_demo_user(self, state: TeamState) -> TeamState:
        if state.team_id <= 0:
            return state
        if state.user_email and state.user_password:
            return state

        base_email = self.args.user_email.strip()
        base_name = self.args.user_name.strip()
        password = self.args.user_password

        for attempt in range(2):
            email = base_email
            if attempt == 1:
                local, _, domain = base_email.partition("@")
                suffix = time.strftime("%m%d%H%M")
                email = f"{local}+{suffix}@{domain or 'optikk.local'}"

            payload = {
                "email": email,
                "name": base_name,
                "password": password,
                "role": "admin",
                "teamIds": [state.team_id],
            }
            try:
                response = http_json(
                    f"{self.args.backend_http.rstrip('/')}/api/v1/users",
                    payload,
                )
                if not response.get("success"):
                    raise RuntimeError(
                        f"user bootstrap failed: {response.get('error', {}).get('message', 'unknown error')}"
                    )
                data = response.get("data") or {}
                self.verify_login(email, password)
                state.user_id = int(data.get("id", 0))
                state.user_email = email
                state.user_password = password
                state.user_name = str(data.get("name") or base_name)
                return state
            except Exception as exc:
                if attempt == 0 and "duplicate" in str(exc).lower():
                    continue
                raise

        raise RuntimeError("failed to bootstrap demo user after retry")

    def verify_login(self, email: str, password: str) -> None:
        response = http_json(
            f"{self.args.backend_http.rstrip('/')}/api/v1/auth/login",
            {"email": email, "password": password},
        )
        if not response.get("success"):
            raise RuntimeError(
                f"login verification failed: {response.get('error', {}).get('message', 'unknown error')}"
            )

    def run(self) -> None:
        team_state = self.bootstrap_team()
        print(
            f"Using team '{team_state.team_name}' (id={team_state.team_id}) with OTLP gRPC endpoint "
            f"{self.args.backend_grpc}",
            flush=True,
        )
        if team_state.user_email and team_state.user_password:
            print(
                f"Login with email={team_state.user_email} password={team_state.user_password}",
                flush=True,
            )

        while True:
            started = time.time()
            try:
                result = self.run_cycle(team_state.api_key)
                print(
                    f"[loop {self.loop_index}] traces={len(result.spans)} spans "
                    f"logs={len(result.logs)} metrics={len(result.metrics)} scenarios="
                    f"{', '.join(s.name for s in result.scenarios)}",
                    flush=True,
                )
                if self.args.once:
                    return
            except Exception as exc:
                if self.args.once:
                    raise
                self.reset_channel()
                print(
                    f"[loop {self.loop_index}] error={exc}. keeping the demo stream alive and retrying.",
                    file=sys.stderr,
                    flush=True,
                )
            finally:
                self.loop_index += 1
            elapsed = time.time() - started
            if elapsed < self.args.interval_sec:
                time.sleep(self.args.interval_sec - elapsed)

    def run_cycle(self, api_key: str) -> "CycleResult":
        spans: list[SpanSpec] = []
        logs: list[LogSpec] = []
        metrics: list[NumberMetricSpec | HistogramMetricSpec] = []
        scenarios: list[ScenarioResult] = []
        base_now_ns = time.time_ns()

        for trace_index in range(self.args.traces_per_interval):
            scenario_name = SCENARIO_ORDER[(self.loop_index * self.args.traces_per_interval + trace_index) % len(SCENARIO_ORDER)]
            scenario = getattr(self, f"scenario_{scenario_name}")
            start_ns = base_now_ns - self.rng.randint(0, max(1, self.args.interval_sec * 700_000_000))
            result = scenario(start_ns)
            scenarios.append(result)
            spans.extend(result.spans)
            logs.extend(result.logs)
            metrics.extend(result.metrics)

        for result in self.root_service_coverage_scenarios(base_now_ns):
            scenarios.append(result)
            spans.extend(result.spans)
            logs.extend(result.logs)
            metrics.extend(result.metrics)

        activity = self.collect_service_activity(spans, scenarios)
        self.advance_service_states(activity)
        metrics.extend(self.resource_health_metrics(base_now_ns))
        export_errors: list[str] = []
        try:
            self.export_traces(spans, api_key)
        except Exception as exc:
            export_errors.append(f"traces export failed: {exc}")
        try:
            self.export_logs(logs, api_key)
        except Exception as exc:
            export_errors.append(f"logs export failed: {exc}")
        try:
            self.export_metrics(metrics, api_key)
        except Exception as exc:
            export_errors.append(f"metrics export failed: {exc}")
        if export_errors:
            raise RuntimeError("; ".join(export_errors))
        return CycleResult(spans=spans, logs=logs, metrics=metrics, scenarios=scenarios)

    def scenario_healthy_checkout(self, start_ns: int) -> ScenarioResult:
        trace_id = self.rand_trace_id()
        request_id = self.rand_token("req")
        user_id = f"user-{self.rng.randint(1000, 1999)}"
        order_id = self.rand_token("ord")
        cart_total = round(self.rng.uniform(48.0, 160.0), 2)

        root_start = start_ns
        root_end = root_start + ms_to_ns(self.rng.randint(140, 260))
        spans: list[SpanSpec] = []
        logs: list[LogSpec] = []
        metrics: list[NumberMetricSpec | HistogramMetricSpec] = []

        gateway = self.new_span(
            SERVICE_RESOURCES["web-gateway"],
            trace_id,
            "",
            "POST /checkout",
            trace_pb2.Span.SPAN_KIND_SERVER,
            root_start,
            root_end,
            {
                "http.method": "POST",
                "http.request.method": "POST",
                "http.route": "/checkout",
                "http.url": "https://demo.optikk.app/checkout",
                "http.response.status_code": 200,
                "enduser.id": user_id,
                "request.id": request_id,
                "order.id": order_id,
                "checkout.total_usd": cart_total,
            },
        )
        spans.append(gateway)
        logs.extend(
            [
                self.make_log(
                    gateway,
                    gateway.start_unix_nano + ms_to_ns(5),
                    "INFO",
                    SEVERITY_INFO,
                    f"Checkout request accepted for {order_id}",
                    {
                        "http.method": "POST",
                        "http.route": "/checkout",
                        "request.id": request_id,
                        "order.id": order_id,
                        "user.id": user_id,
                    },
                ),
                self.make_log(
                    gateway,
                    gateway.end_unix_nano - ms_to_ns(8),
                    "INFO",
                    SEVERITY_INFO,
                    "Checkout completed successfully",
                    {
                        "http.method": "POST",
                        "http.route": "/checkout",
                        "request.id": request_id,
                        "order.id": order_id,
                    },
                ),
            ]
        )

        checkout_client = self.child_client_span(
            SERVICE_RESOURCES["web-gateway"],
            SERVICE_RESOURCES["checkout-api"],
            trace_id,
            gateway.span_id,
            "HTTP POST /api/checkout",
            gateway.start_unix_nano + ms_to_ns(8),
            110,
            "POST",
            "/api/checkout",
            200,
            {"request.id": request_id, "order.id": order_id},
        )
        checkout_server = self.mirrored_server_span(
            SERVICE_RESOURCES["checkout-api"],
            trace_id,
            checkout_client.span_id,
            "POST /api/checkout",
            checkout_client.start_unix_nano + ms_to_ns(3),
            100,
            "POST",
            "/api/checkout",
            200,
            {"request.id": request_id, "order.id": order_id},
        )
        spans.extend([checkout_client, checkout_server])
        logs.append(
            self.make_log(
                checkout_server,
                checkout_server.start_unix_nano + ms_to_ns(15),
                "INFO",
                SEVERITY_INFO,
                "Pricing and inventory checks started",
                {"request.id": request_id, "order.id": order_id},
            )
        )

        catalog_client = self.child_client_span(
            SERVICE_RESOURCES["checkout-api"],
            SERVICE_RESOURCES["catalog-api"],
            trace_id,
            checkout_server.span_id,
            "HTTP GET /api/catalog/items",
            checkout_server.start_unix_nano + ms_to_ns(6),
            34,
            "GET",
            "/api/catalog/items",
            200,
            {"http.query.sku_count": "3", "request.id": request_id},
        )
        catalog_server = self.mirrored_server_span(
            SERVICE_RESOURCES["catalog-api"],
            trace_id,
            catalog_client.span_id,
            "GET /api/catalog/items",
            catalog_client.start_unix_nano + ms_to_ns(2),
            28,
            "GET",
            "/api/catalog/items",
            200,
            {"request.id": request_id},
        )
        catalog_db = self.db_span(
            SERVICE_RESOURCES["catalog-api"],
            trace_id,
            catalog_server.span_id,
            "SELECT catalog products",
            catalog_server.start_unix_nano + ms_to_ns(4),
            12,
            "postgresql",
            "catalog",
            "SELECT",
            "ok",
        )
        spans.extend([catalog_client, catalog_server, catalog_db])

        payment_client = self.child_client_span(
            SERVICE_RESOURCES["checkout-api"],
            SERVICE_RESOURCES["payment-api"],
            trace_id,
            checkout_server.span_id,
            "HTTP POST /api/payments/authorize",
            checkout_server.start_unix_nano + ms_to_ns(48),
            41,
            "POST",
            "/api/payments/authorize",
            200,
            {"payment.amount_usd": cart_total, "request.id": request_id},
        )
        payment_server = self.mirrored_server_span(
            SERVICE_RESOURCES["payment-api"],
            trace_id,
            payment_client.span_id,
            "POST /api/payments/authorize",
            payment_client.start_unix_nano + ms_to_ns(2),
            36,
            "POST",
            "/api/payments/authorize",
            200,
            {"request.id": request_id, "order.id": order_id},
        )
        stripe_span = self.child_external_client_span(
            SERVICE_RESOURCES["payment-api"],
            trace_id,
            payment_server.span_id,
            "Stripe: POST /v1/charges",
            payment_server.start_unix_nano + ms_to_ns(5),
            28,
            "POST",
            "api.stripe.com",
            "/v1/charges",
            200,
        )
        redis_span = self.redis_span(
            SERVICE_RESOURCES["payment-api"],
            trace_id,
            payment_server.span_id,
            "GET payment-profile cache",
            payment_server.start_unix_nano + ms_to_ns(5),
            5,
            "GET",
        )
        kafka_publish = self.kafka_span(
            SERVICE_RESOURCES["checkout-api"],
            trace_id,
            checkout_server.span_id,
            "publish order.created",
            trace_pb2.Span.SPAN_KIND_PRODUCER,
            checkout_server.end_unix_nano - ms_to_ns(16),
            8,
            "publish",
            "orders.events",
            "checkout-consumers",
        )
        spans.extend([payment_client, payment_server, stripe_span, redis_span, kafka_publish])

        logs.extend(
            [
                self.make_log(
                    payment_server,
                    payment_server.start_unix_nano + ms_to_ns(10),
                    "INFO",
                    SEVERITY_INFO,
                    "Payment authorization approved",
                    {"order.id": order_id, "payment.provider": "stripe"},
                ),
                self.make_log(
                    kafka_publish,
                    kafka_publish.start_unix_nano + ms_to_ns(3),
                    "INFO",
                    SEVERITY_INFO,
                    "Published order.created event",
                    {"messaging.destination.name": "orders.events", "order.id": order_id},
                ),
            ]
        )

        metrics.extend(
            self.http_metrics_from_span(gateway, 1200, 3200)
            + self.http_metrics_from_span(checkout_server, 820, 2100)
            + self.http_metrics_from_span(catalog_server, 300, 2400)
            + self.http_metrics_from_span(payment_server, 620, 1800)
            + self.db_metrics_from_span(catalog_db)
            + self.redis_metrics("payment-api", hits=24, misses=3, memory_used=146_000_000, commands=220, evicted=0)
            + self.kafka_metrics("checkout-api", lag=6, lag_sum=22, published=18, received=14, assigned=6)
        )
        return ScenarioResult("healthy_checkout", spans, logs, metrics)

    def scenario_slow_db_checkout(self, start_ns: int) -> ScenarioResult:
        result = self.scenario_healthy_checkout(start_ns)
        result.name = "slow_db_checkout"
        for span in result.spans:
            if span.name == "SELECT catalog products":
                span.end_unix_nano = span.start_unix_nano + ms_to_ns(180)
                span.attributes["db.statement.class"] = "inventory_join"
                span.attributes["db.rows_scanned"] = 18920
            if span.name == "GET /api/catalog/items":
                span.end_unix_nano += ms_to_ns(140)
            if span.name == "HTTP GET /api/catalog/items":
                span.end_unix_nano += ms_to_ns(145)
            if span.name == "POST /api/checkout":
                span.end_unix_nano += ms_to_ns(140)
            if span.name == "HTTP POST /api/checkout":
                span.end_unix_nano += ms_to_ns(145)
            if span.name == "POST /checkout":
                span.end_unix_nano += ms_to_ns(150)
        result.logs.append(
            self.make_log(
                next(span for span in result.spans if span.name == "SELECT catalog products"),
                next(span for span in result.spans if span.name == "SELECT catalog products").end_unix_nano - ms_to_ns(12),
                "WARN",
                SEVERITY_WARN,
                "Catalog query crossed slow threshold",
                {"db.system": "postgresql", "db.namespace": "catalog", "slow.query.ms": 180},
            )
        )
        result.metrics.extend(
            self.db_connection_metrics("catalog-api", utilization=0.88, active=29, max_conn=32)
        )
        return result

    def scenario_payment_failure(self, start_ns: int) -> ScenarioResult:
        trace_id = self.rand_trace_id()
        request_id = self.rand_token("req")
        order_id = self.rand_token("ord")

        root = self.new_span(
            SERVICE_RESOURCES["web-gateway"],
            trace_id,
            "",
            "POST /checkout",
            trace_pb2.Span.SPAN_KIND_SERVER,
            start_ns,
            start_ns + ms_to_ns(290),
            {
                "http.method": "POST",
                "http.request.method": "POST",
                "http.route": "/checkout",
                "http.response.status_code": 502,
                "request.id": request_id,
                "order.id": order_id,
            },
            status_code=STATUS_ERROR,
            status_message="Payment authorization failed",
        )
        payment_client = self.child_client_span(
            SERVICE_RESOURCES["web-gateway"],
            SERVICE_RESOURCES["payment-api"],
            trace_id,
            root.span_id,
            "HTTP POST /api/payments/authorize",
            root.start_unix_nano + ms_to_ns(14),
            102,
            "POST",
            "/api/payments/authorize",
            502,
            {"request.id": request_id, "order.id": order_id},
            otel_status_code=STATUS_ERROR,
            status_message="gateway upstream returned 502",
        )
        payment_server = self.mirrored_server_span(
            SERVICE_RESOURCES["payment-api"],
            trace_id,
            payment_client.span_id,
            "POST /api/payments/authorize",
            payment_client.start_unix_nano + ms_to_ns(4),
            95,
            "POST",
            "/api/payments/authorize",
            502,
            {"request.id": request_id, "order.id": order_id},
            status_code=STATUS_ERROR,
            status_message="card processor timeout",
        )
        stripe_span = self.child_external_client_span(
            SERVICE_RESOURCES["payment-api"],
            trace_id,
            payment_server.span_id,
            "Stripe: POST /v1/charges",
            payment_server.start_unix_nano + ms_to_ns(5),
            88,
            "POST",
            "api.stripe.com",
            "/v1/charges",
            504,
            otel_status_code=STATUS_ERROR,
            status_message="gateway timeout",
        )
        payment_server.attributes.update(
            {
                "exception.type": "GatewayTimeoutError",
                "exception.message": "processor timeout after 90ms",
                "exception.stacktrace": "GatewayTimeoutError: processor timeout\n  at provider.go:88",
                "exception.escaped": True,
                "error.type": "gateway_timeout",
            }
        )
        payment_server.events.append(
            SpanEventSpec(
                name="exception",
                time_unix_nano=payment_server.end_unix_nano - ms_to_ns(2),
                attributes={
                    "exception.type": "GatewayTimeoutError",
                    "exception.message": "processor timeout after 90ms",
                },
            )
        )
        spans = [root, payment_client, payment_server, stripe_span]
        logs = [
            self.make_log(
                payment_server,
                payment_server.start_unix_nano + ms_to_ns(40),
                "ERROR",
                SEVERITY_ERROR,
                "Card processor timeout while authorizing payment",
                {
                    "request.id": request_id,
                    "order.id": order_id,
                    "error.type": "gateway_timeout",
                    "http.method": "POST",
                    "http.route": "/api/payments/authorize",
                },
            ),
            self.make_log(
                root,
                root.end_unix_nano - ms_to_ns(3),
                "ERROR",
                SEVERITY_ERROR,
                "Checkout failed because payment authorization did not complete",
                {
                    "request.id": request_id,
                    "order.id": order_id,
                    "http.method": "POST",
                    "http.route": "/checkout",
                },
            ),
        ]
        metrics = (
            self.http_metrics_from_span(root, 1200, 1800)
            + self.http_metrics_from_span(payment_server, 640, 960)
            + self.kafka_metrics("orders-worker", lag=24, lag_sum=92, published=2, received=0, assigned=3)
        )
        return ScenarioResult("payment_failure", spans, logs, metrics)

    def scenario_order_worker_kafka_lag(self, start_ns: int) -> ScenarioResult:
        trace_id = self.rand_trace_id()
        root = self.new_span(
            SERVICE_RESOURCES["orders-worker"],
            trace_id,
            "",
            "orders-worker consumer loop",
            trace_pb2.Span.SPAN_KIND_CONSUMER,
            start_ns,
            start_ns + ms_to_ns(420),
            {
                "messaging.system": "kafka",
                "messaging.destination.name": "orders.events",
                "messaging.consumer.group.name": "orders-worker",
                "messaging.operation.name": "process",
            },
        )
        receive = self.kafka_span(
            SERVICE_RESOURCES["orders-worker"],
            trace_id,
            root.span_id,
            "receive order.created",
            trace_pb2.Span.SPAN_KIND_CONSUMER,
            root.start_unix_nano + ms_to_ns(5),
            75,
            "receive",
            "orders.events",
            "orders-worker",
        )
        process = self.new_span(
            SERVICE_RESOURCES["orders-worker"],
            trace_id,
            root.span_id,
            "process order.created",
            trace_pb2.Span.SPAN_KIND_INTERNAL,
            root.start_unix_nano + ms_to_ns(92),
            root.start_unix_nano + ms_to_ns(360),
            {
                "messaging.system": "kafka",
                "messaging.destination.name": "orders.events",
                "messaging.consumer.group.name": "orders-worker",
                "order.batch.size": 12,
            },
        )
        db = self.db_span(
            SERVICE_RESOURCES["orders-worker"],
            trace_id,
            process.span_id,
            "INSERT order projections",
            process.start_unix_nano + ms_to_ns(18),
            110,
            "postgresql",
            "orders",
            "INSERT",
            "ok",
        )
        sqs_span = self.child_external_client_span(
            SERVICE_RESOURCES["orders-worker"],
            trace_id,
            process.span_id,
            "SQS: SendMessage",
            process.start_unix_nano + ms_to_ns(140),
            45,
            "POST",
            "sqs.us-east-1.amazonaws.com",
            "/",
            200,
        )
        fcm_span = self.child_external_client_span(
            SERVICE_RESOURCES["orders-worker"],
            trace_id,
            process.span_id,
            "FCM: Send Notification",
            process.start_unix_nano + ms_to_ns(200),
            65,
            "POST",
            "fcm.googleapis.com",
            "/v1/projects/optikk-demo/messages:send",
            200,
        )
        spans = [root, receive, process, db, sqs_span, fcm_span]
        logs = [
            self.make_log(
                process,
                process.start_unix_nano + ms_to_ns(15),
                "WARN",
                SEVERITY_WARN,
                "Consumer lag rising on orders.events",
                {
                    "messaging.destination.name": "orders.events",
                    "messaging.consumer.group.name": "orders-worker",
                    "lag.partitions": 8,
                },
            )
        ]
        metrics = self.kafka_metrics("orders-worker", lag=108, lag_sum=420, published=4, received=11, assigned=12)
        metrics.extend(self.db_metrics_from_span(db))
        return ScenarioResult("order_worker_kafka_lag", spans, logs, metrics)

    def scenario_redis_pressure(self, start_ns: int) -> ScenarioResult:
        trace_id = self.rand_trace_id()
        request_id = self.rand_token("req")
        root = self.new_span(
            SERVICE_RESOURCES["checkout-api"],
            trace_id,
            "",
            "GET /api/cart",
            trace_pb2.Span.SPAN_KIND_SERVER,
            start_ns,
            start_ns + ms_to_ns(185),
            {
                "http.method": "GET",
                "http.request.method": "GET",
                "http.route": "/api/cart",
                "http.response.status_code": 200,
                "request.id": request_id,
            },
        )
        redis = self.redis_span(
            SERVICE_RESOURCES["checkout-api"],
            trace_id,
            root.span_id,
            "GET cart-session cache",
            root.start_unix_nano + ms_to_ns(10),
            80,
            "GET",
            hit=False,
        )
        fallback_db = self.db_span(
            SERVICE_RESOURCES["checkout-api"],
            trace_id,
            root.span_id,
            "SELECT cart recovery",
            root.start_unix_nano + ms_to_ns(95),
            42,
            "postgresql",
            "checkout",
            "SELECT",
            "ok",
        )
        spans = [root, redis, fallback_db]
        logs = [
            self.make_log(
                redis,
                redis.end_unix_nano - ms_to_ns(4),
                "WARN",
                SEVERITY_WARN,
                "Redis cache miss forced cart reload from primary store",
                {
                    "request.id": request_id,
                    "cache.system": "redis",
                    "cache.hit": False,
                    "http.route": "/api/cart",
                },
            )
        ]
        metrics = self.http_metrics_from_span(root, 420, 2100)
        metrics.extend(self.db_metrics_from_span(fallback_db))
        metrics.extend(self.redis_metrics("checkout-api", hits=12, misses=14, memory_used=612_000_000, commands=340, evicted=11))
        return ScenarioResult("redis_pressure", spans, logs, metrics)

    def scenario_ai_support_chat(self, start_ns: int) -> ScenarioResult:
        trace_id = self.rand_trace_id()
        conversation_id = self.pick_conversation_id()
        turn_no = self.ai_conversation_turns[conversation_id] + 1
        self.ai_conversation_turns[conversation_id] = turn_no
        user_prompt = f"Summarize why order ORD-{1000 + turn_no} is delayed and suggest the next action."
        assistant_reply = "The delay is tied to a payment retry and an inventory sync lag. I would retry payment capture and notify the shopper."
        model = AI_MODELS[self.loop_index % len(AI_MODELS)]

        root = self.new_span(
            SERVICE_RESOURCES["recommendation-ai"],
            trace_id,
            "",
            "ai support request",
            trace_pb2.Span.SPAN_KIND_SERVER,
            start_ns,
            start_ns + ms_to_ns(650),
            {
                "http.method": "POST",
                "http.request.method": "POST",
                "http.route": "/ai/support-chat",
                "http.response.status_code": 200,
                "gen_ai.conversation.id": conversation_id,
            },
        )
        chain = self.new_span(
            SERVICE_RESOURCES["recommendation-ai"],
            trace_id,
            root.span_id,
            "support chain orchestrator",
            trace_pb2.Span.SPAN_KIND_INTERNAL,
            root.start_unix_nano + ms_to_ns(8),
            root.start_unix_nano + ms_to_ns(620),
            {"gen_ai.conversation.id": conversation_id},
        )
        retriever = self.new_span(
            SERVICE_RESOURCES["recommendation-ai"],
            trace_id,
            chain.span_id,
            "retriever order history",
            trace_pb2.Span.SPAN_KIND_INTERNAL,
            chain.start_unix_nano + ms_to_ns(14),
            chain.start_unix_nano + ms_to_ns(110),
            {"gen_ai.conversation.id": conversation_id},
        )
        tool = self.new_span(
            SERVICE_RESOURCES["recommendation-ai"],
            trace_id,
            chain.span_id,
            "tool inventory lookup",
            trace_pb2.Span.SPAN_KIND_INTERNAL,
            chain.start_unix_nano + ms_to_ns(118),
            chain.start_unix_nano + ms_to_ns(220),
            {"gen_ai.conversation.id": conversation_id},
        )
        llm = self.new_span(
            SERVICE_RESOURCES["recommendation-ai"],
            trace_id,
            chain.span_id,
            "chat completion",
            trace_pb2.Span.SPAN_KIND_CLIENT,
            chain.start_unix_nano + ms_to_ns(240),
            chain.start_unix_nano + ms_to_ns(565),
            {
                "gen_ai.request.model": model,
                "server.address": "api.openai.com",
                "gen_ai.operation.name": "chat.completions",
                "gen_ai.usage.input_tokens": 820 + turn_no * 11,
                "gen_ai.usage.output_tokens": 210 + turn_no * 5,
                "gen_ai.response.finish_reasons": "stop",
                "gen_ai.conversation.id": conversation_id,
            },
            events=[
                SpanEventSpec(
                    "gen_ai.content.prompt",
                    chain.start_unix_nano + ms_to_ns(248),
                    {
                        "gen_ai.prompt": json.dumps(
                            [
                                {"role": "system", "content": "You help customer support agents explain order delays."},
                                {"role": "user", "content": user_prompt},
                            ]
                        )
                    },
                ),
                SpanEventSpec(
                    "gen_ai.content.completion",
                    chain.start_unix_nano + ms_to_ns(560),
                    {
                        "gen_ai.completion": json.dumps(
                            [
                                {"role": "assistant", "content": assistant_reply},
                            ]
                        )
                    },
                ),
            ],
        )
        spans = [root, chain, retriever, tool, llm]
        logs = [
            self.make_log(
                chain,
                chain.start_unix_nano + ms_to_ns(24),
                "INFO",
                SEVERITY_INFO,
                f"AI conversation {conversation_id} turn {turn_no} started",
                {"gen_ai.conversation.id": conversation_id, "turn.number": turn_no},
            ),
            self.make_log(
                llm,
                llm.end_unix_nano - ms_to_ns(10),
                "INFO",
                SEVERITY_INFO,
                "Model response generated successfully",
                {
                    "gen_ai.conversation.id": conversation_id,
                    "gen_ai.request.model": model,
                    "gen_ai.usage.total_tokens": 1030 + turn_no * 16,
                },
            ),
        ]
        metrics = self.http_metrics_from_span(root, 1800, 4900)
        metrics.extend(self.ai_metrics(llm, model=model))
        metrics.extend(self.kafka_metrics("recommendation-ai", lag=3, lag_sum=9, published=1, received=2, assigned=2))
        return ScenarioResult("ai_support_chat", spans, logs, metrics)

    def root_service_coverage_scenarios(self, base_now_ns: int) -> list[ScenarioResult]:
        return [
            self.service_root_http_scenario(
                scenario_name="catalog_api_root",
                service_name="catalog-api",
                start_ns=base_now_ns - ms_to_ns(80),
                operation_name="GET /api/catalog/featured",
                method="GET",
                route="/api/catalog/featured",
                duration_ms=62,
                http_status_code=200,
                request_bytes=280,
                response_bytes=3600,
                extra_attrs={"feature.collection": "homepage"},
                db_config={"name": "SELECT featured products", "duration_ms": 18, "namespace": "catalog", "operation": "SELECT"},
            ),
            self.service_root_http_scenario(
                scenario_name="payment_api_root",
                service_name="payment-api",
                start_ns=base_now_ns - ms_to_ns(145),
                operation_name="POST /api/payments/reconcile",
                method="POST",
                route="/api/payments/reconcile",
                duration_ms=96,
                http_status_code=500 if self.loop_index % 3 == 0 else 202,
                request_bytes=740,
                response_bytes=980,
                extra_attrs={"reconcile.batch.id": self.rand_token("batch")},
                redis_config={"operation": "GET", "hits": 18, "misses": 2, "memory_used": 174_000_000, "commands": 140, "evicted": 0},
                error_message="Gateway replay window expired",
            ),
            self.service_root_http_scenario(
                scenario_name="auth_api_root",
                service_name="auth-api",
                start_ns=base_now_ns - ms_to_ns(210),
                operation_name="POST /api/auth/session/refresh",
                method="POST",
                route="/api/auth/session/refresh",
                duration_ms=54,
                http_status_code=401 if self.loop_index % 5 == 1 else 200,
                request_bytes=420,
                response_bytes=620,
                extra_attrs={"auth.provider": "password"},
                db_config={"name": "SELECT session record", "duration_ms": 12, "namespace": "identity", "operation": "SELECT"},
                redis_config={"operation": "GET", "hits": 30, "misses": 1, "memory_used": 126_000_000, "commands": 160, "evicted": 0},
                error_message="Session token expired",
            ),
            self.service_root_http_scenario(
                scenario_name="inventory_api_root",
                service_name="inventory-api",
                start_ns=base_now_ns - ms_to_ns(275),
                operation_name="POST /api/inventory/reservations",
                method="POST",
                route="/api/inventory/reservations",
                duration_ms=88,
                http_status_code=409 if self.loop_index % 6 == 3 else 200,
                request_bytes=560,
                response_bytes=1040,
                extra_attrs={"warehouse.id": f"wh-{(self.loop_index % 3) + 1}"},
                db_config={"name": "UPDATE inventory reservation", "duration_ms": 22, "namespace": "inventory", "operation": "UPDATE"},
                kafka_config={"lag": 8, "lag_sum": 24, "published": 4, "received": 1, "assigned": 4},
                error_message="Inventory reservation conflict",
            ),
            self.service_root_http_scenario(
                scenario_name="notification_api_root",
                service_name="notification-api",
                start_ns=base_now_ns - ms_to_ns(340),
                operation_name="POST /api/notifications/send",
                method="POST",
                route="/api/notifications/send",
                duration_ms=78,
                http_status_code=503 if self.loop_index % 4 == 1 else 202,
                request_bytes=690,
                response_bytes=540,
                extra_attrs={"notification.channel": "email"},
                kafka_config={"lag": 10, "lag_sum": 30, "published": 8, "received": 3, "assigned": 4},
                external_host="api.sendgrid.com",
                external_path="/v3/mail/send",
                error_message="Email provider unavailable",
            ),
            self.service_root_http_scenario(
                scenario_name="search_api_root",
                service_name="search-api",
                start_ns=base_now_ns - ms_to_ns(405),
                operation_name="GET /api/search",
                method="GET",
                route="/api/search",
                duration_ms=72,
                http_status_code=429 if self.loop_index % 8 == 5 else 200,
                request_bytes=220,
                response_bytes=4200,
                extra_attrs={"search.query.length": 14},
                db_config={"name": "SELECT search documents", "duration_ms": 19, "namespace": "search", "operation": "SELECT"},
                redis_config={"operation": "GET", "hits": 42, "misses": 5, "memory_used": 224_000_000, "commands": 260, "evicted": 1},
                external_host="api.github.com",
                external_path="/search/repositories",
                error_message="Query rate limit exceeded",
            ),
        ]

    def service_root_http_scenario(
        self,
        *,
        scenario_name: str,
        service_name: str,
        start_ns: int,
        operation_name: str,
        method: str,
        route: str,
        duration_ms: int,
        http_status_code: int,
        request_bytes: int,
        response_bytes: int,
        extra_attrs: dict[str, Any] | None = None,
        db_config: dict[str, Any] | None = None,
        redis_config: dict[str, Any] | None = None,
        kafka_config: dict[str, Any] | None = None,
        external_host: str | None = None,
        external_path: str | None = None,
        external_duration_ms: int = 45,
        error_message: str = "",
    ) -> ScenarioResult:
        resource = SERVICE_RESOURCES[service_name]
        trace_id = self.rand_trace_id()
        request_id = self.rand_token("req")
        attrs = {
            "http.method": method,
            "http.request.method": method,
            "http.route": route,
            "http.response.status_code": http_status_code,
            "request.id": request_id,
        }
        attrs.update(extra_attrs or {})
        root_status_code = STATUS_ERROR if http_status_code >= 400 else STATUS_OK
        root_status_message = error_message if root_status_code == STATUS_ERROR else ""
        root = self.new_span(
            resource,
            trace_id,
            "",
            operation_name,
            trace_pb2.Span.SPAN_KIND_SERVER,
            start_ns,
            start_ns + ms_to_ns(duration_ms),
            attrs,
            status_code=root_status_code,
            status_message=root_status_message,
        )
        if root_status_code == STATUS_ERROR:
            root.attributes["error.type"] = slugify(error_message).replace("-", "_") or "request_error"
            if http_status_code >= 500:
                root.attributes["exception.type"] = "UpstreamServiceError"
                root.attributes["exception.message"] = error_message or "upstream dependency failed"
                root.events.append(
                    SpanEventSpec(
                        name="exception",
                        time_unix_nano=root.end_unix_nano - ms_to_ns(1),
                        attributes={"exception.type": "UpstreamServiceError", "exception.message": error_message or "request failed"},
                    )
                )

        spans = [root]
        logs = [
            self.make_log(
                root,
                root.start_unix_nano + ms_to_ns(4),
                "INFO",
                SEVERITY_INFO,
                f"{service_name} received {method} {route}",
                {"request.id": request_id, "http.method": method, "http.route": route},
            )
        ]
        metrics = self.http_metrics_from_span(root, request_bytes, response_bytes)

        if db_config:
            db_span = self.db_span(
                resource,
                trace_id,
                root.span_id,
                str(db_config["name"]),
                root.start_unix_nano + ms_to_ns(8),
                int(db_config["duration_ms"]),
                "postgresql",
                str(db_config["namespace"]),
                str(db_config["operation"]),
                "error" if root_status_code == STATUS_ERROR and http_status_code >= 500 else "ok",
            )
            spans.append(db_span)
            metrics.extend(self.db_metrics_from_span(db_span))

        if redis_config:
            redis_span = self.redis_span(
                resource,
                trace_id,
                root.span_id,
                f"{redis_config['operation']} session cache",
                root.start_unix_nano + ms_to_ns(10),
                6,
                str(redis_config["operation"]),
                hit=int(redis_config.get("misses", 0)) == 0,
            )
            spans.append(redis_span)
            metrics.extend(
                self.redis_metrics(
                    service_name,
                    hits=int(redis_config["hits"]),
                    misses=int(redis_config["misses"]),
                    memory_used=int(redis_config["memory_used"]),
                    commands=int(redis_config["commands"]),
                    evicted=int(redis_config["evicted"]),
                )
            )

        if external_host:
            ext_status = 200
            ext_otel = STATUS_OK
            if root_status_code == STATUS_ERROR and http_status_code >= 500 and self.rng.random() > 0.5:
                ext_status = 503
                ext_otel = STATUS_ERROR

            ext_span = self.child_external_client_span(
                resource,
                trace_id,
                root.span_id,
                f"External API: {external_host}",
                root.start_unix_nano + ms_to_ns(12),
                external_duration_ms,
                "POST" if method == "POST" else "GET",
                external_host,
                external_path or "/api/v1",
                ext_status,
                otel_status_code=ext_otel,
            )
            spans.append(ext_span)

        if kafka_config:
            kafka_span = self.kafka_span(
                resource,
                trace_id,
                root.span_id,
                f"publish {service_name}.events",
                trace_pb2.Span.SPAN_KIND_PRODUCER,
                root.end_unix_nano - ms_to_ns(12),
                7,
                "publish",
                f"{service_name}.events",
                service_name,
            )
            spans.append(kafka_span)
            metrics.extend(
                self.kafka_metrics(
                    service_name,
                    lag=int(kafka_config["lag"]),
                    lag_sum=int(kafka_config["lag_sum"]),
                    published=int(kafka_config["published"]),
                    received=int(kafka_config["received"]),
                    assigned=int(kafka_config["assigned"]),
                )
            )

        logs.append(
            self.make_log(
                root,
                root.end_unix_nano - ms_to_ns(3),
                "ERROR" if root_status_code == STATUS_ERROR else "INFO",
                SEVERITY_ERROR if root_status_code == STATUS_ERROR else SEVERITY_INFO,
                error_message if root_status_code == STATUS_ERROR else f"{service_name} completed {route}",
                {
                    "request.id": request_id,
                    "http.method": method,
                    "http.route": route,
                    "http.response.status_code": http_status_code,
                },
            )
        )
        return ScenarioResult(scenario_name, spans, logs, metrics)

    def collect_service_activity(
        self,
        spans: list[SpanSpec],
        scenarios: list[ScenarioResult],
    ) -> dict[str, ServiceActivity]:
        activity = {service_name: ServiceActivity() for service_name in SERVICE_RESOURCES}
        scenario_counts = defaultdict(int)
        for scenario in scenarios:
            scenario_counts[scenario.name] += 1

        for span in spans:
            service_name = span.resource.service_name
            service_activity = activity[service_name]
            duration_ms = max(1.0, ns_to_ms(span.end_unix_nano - span.start_unix_nano))
            http_status = int(span.attributes.get("http.response.status_code", 0) or 0)
            db_system = str(span.attributes.get("db.system", ""))
            messaging_system = str(span.attributes.get("messaging.system", ""))
            operation_name = str(span.attributes.get("messaging.operation.name", ""))

            if span.kind == trace_pb2.Span.SPAN_KIND_SERVER and "http.route" in span.attributes:
                service_activity.http_requests += 1
                service_activity.total_http_duration_ms += duration_ms
                if http_status >= 500 or span.status_code == STATUS_ERROR:
                    service_activity.http_errors += 1

            if db_system and db_system != "redis":
                service_activity.db_calls += 1
                service_activity.db_duration_ms += duration_ms

            if db_system == "redis":
                service_activity.redis_calls += 1
                if span.attributes.get("cache.hit") is False:
                    service_activity.redis_misses += 1

            if messaging_system == "kafka":
                if span.kind == trace_pb2.Span.SPAN_KIND_PRODUCER:
                    service_activity.kafka_publishes += 1
                if span.kind == trace_pb2.Span.SPAN_KIND_CONSUMER:
                    service_activity.kafka_receives += 1
                if operation_name == "process" or "process" in span.name.lower():
                    service_activity.kafka_processes += 1

            if "gen_ai.request.model" in span.attributes:
                service_activity.ai_requests += 1
                service_activity.ai_input_tokens += int(span.attributes.get("gen_ai.usage.input_tokens", 0) or 0)
                service_activity.ai_output_tokens += int(span.attributes.get("gen_ai.usage.output_tokens", 0) or 0)

        checkout_load = scenario_counts["healthy_checkout"] + scenario_counts["slow_db_checkout"] + scenario_counts["payment_failure"]
        activity["web-gateway"].http_requests += checkout_load
        activity["checkout-api"].http_requests += checkout_load + scenario_counts["redis_pressure"]
        activity["catalog-api"].db_calls += scenario_counts["healthy_checkout"] + (2 * scenario_counts["slow_db_checkout"])
        activity["payment-api"].http_errors += scenario_counts["payment_failure"]
        activity["orders-worker"].kafka_receives += 4 * scenario_counts["order_worker_kafka_lag"]
        activity["orders-worker"].kafka_processes += 2 * scenario_counts["order_worker_kafka_lag"]
        activity["orders-worker"].db_calls += scenario_counts["order_worker_kafka_lag"]
        activity["checkout-api"].redis_misses += 2 * scenario_counts["redis_pressure"]
        activity["recommendation-ai"].ai_requests += scenario_counts["ai_support_chat"]
        activity["recommendation-ai"].ai_input_tokens += 900 * scenario_counts["ai_support_chat"]
        activity["recommendation-ai"].ai_output_tokens += 240 * scenario_counts["ai_support_chat"]
        return activity

    def advance_service_states(self, activity: dict[str, ServiceActivity]) -> None:
        for service_name, state in self.service_states.items():
            profile = SERVICE_PROFILES[service_name]
            sample = activity.get(service_name, ServiceActivity())
            interval = max(float(self.args.interval_sec), 1.0)
            request_rate = sample.http_requests / interval
            avg_http_ms = sample.total_http_duration_ms / max(sample.http_requests, 1)
            avg_db_ms = sample.db_duration_ms / max(sample.db_calls, 1) if sample.db_calls else state.db_use_ms
            redis_miss_ratio = sample.redis_misses / max(sample.redis_calls, 1) if sample.redis_calls else 0.03
            ai_token_pressure = (sample.ai_input_tokens + sample.ai_output_tokens) / 2500.0
            error_pressure = sample.http_errors / max(sample.http_requests, 1) if sample.http_requests else 0.0
            kafka_backlog_pressure = max(sample.kafka_receives - sample.kafka_processes, 0)

            cpu_target = profile.base_cpu_util
            cpu_target += request_rate * 0.035
            cpu_target += avg_http_ms / 3000.0
            cpu_target += sample.db_calls * 0.018
            cpu_target += kafka_backlog_pressure * 0.025
            cpu_target += ai_token_pressure * 0.11
            cpu_target += error_pressure * 0.16
            cpu_target = clamp(cpu_target + self.rng.uniform(-0.015, 0.02), 0.07, 0.96)

            mem_target = profile.base_memory_util
            mem_target += request_rate * 0.014
            mem_target += sample.db_calls * 0.008
            mem_target += sample.redis_calls * 0.01
            mem_target += ai_token_pressure * 0.05
            mem_target = clamp(mem_target + self.rng.uniform(-0.01, 0.015), 0.18, 0.94)

            disk_target = profile.base_disk_util + sample.db_calls * 0.006 + sample.kafka_publishes * 0.004
            disk_target = clamp(disk_target + self.rng.uniform(-0.006, 0.012), 0.12, 0.92)

            net_target = profile.base_network_util
            net_target += request_rate * 0.028
            net_target += (sample.kafka_publishes + sample.kafka_receives) * 0.012
            net_target += ai_token_pressure * 0.03
            net_target = clamp(net_target + self.rng.uniform(-0.01, 0.015), 0.08, 0.88)

            state.cpu_util = smooth_towards(state.cpu_util, cpu_target, 0.42)
            state.mem_util = smooth_towards(state.mem_util, mem_target, 0.24)
            state.disk_util = smooth_towards(state.disk_util, disk_target, 0.12)
            state.net_util = smooth_towards(state.net_util, net_target, 0.38)
            state.process_cpu_usage = clamp(state.cpu_util + 0.04 + ai_token_pressure * 0.03, 0.08, 0.99)
            state.request_rate = smooth_towards(state.request_rate, request_rate, 0.5)
            state.active_requests = max(
                1,
                int(round(smooth_towards(float(state.active_requests), 1.0 + request_rate * 2.8 + error_pressure * 4.0, 0.48))),
            )
            state.connections = max(
                profile.connection_base,
                int(round(smooth_towards(float(state.connections), profile.connection_base + request_rate * 7.0 + sample.redis_calls * 1.8, 0.32))),
            )
            state.fd_count = max(
                profile.fd_base,
                int(round(smooth_towards(float(state.fd_count), profile.fd_base + state.connections * 1.4 + sample.db_calls * 2.2, 0.2))),
            )
            state.memory_used = int(profile.memory_total * state.mem_util)
            state.disk_free = max(profile.disk_total - int(profile.disk_total * state.disk_util), 0)
            state.rx_bytes_per_sec = int(profile.network_capacity_bytes * state.net_util * (0.58 + self.rng.uniform(-0.04, 0.06)))
            state.tx_bytes_per_sec = int(profile.network_capacity_bytes * state.net_util * (0.51 + self.rng.uniform(-0.04, 0.05)))

            if profile.db_pool_max:
                db_active_target = clamp(2.0 + sample.db_calls * 2.4 + avg_db_ms / 22.0 + request_rate * 1.6, 1.0, float(profile.db_pool_max))
                db_pending_target = clamp(max(avg_db_ms - 35.0, 0.0) / 18.0 + sample.http_errors * 1.5, 0.0, float(profile.db_pool_max // 2))
                state.db_active = int(round(smooth_towards(float(state.db_active), db_active_target, 0.38)))
                state.db_pending = int(round(smooth_towards(float(state.db_pending), db_pending_target, 0.28)))
                state.db_wait_ms = smooth_towards(state.db_wait_ms, max(1.4, avg_db_ms * 0.26 + state.db_pending * 1.5), 0.42)
                state.db_use_ms = smooth_towards(state.db_use_ms, max(4.0, avg_db_ms * 0.82), 0.36)

            if profile.redis_memory_floor:
                redis_clients_target = profile.connection_base * 0.55 + sample.redis_calls * 4.0 + sample.http_requests * 1.5
                redis_memory_target = profile.redis_memory_floor + int(sample.redis_calls * 12_000_000 + sample.redis_misses * 26_000_000)
                redis_keys_target = profile.redis_keys_floor + sample.redis_calls * 35 - sample.redis_misses * 12
                redis_expires_target = max(profile.redis_keys_floor // 4 + sample.redis_calls * 8, 200)
                state.redis_clients = int(round(smooth_towards(float(state.redis_clients), redis_clients_target, 0.34)))
                state.redis_memory_used = int(round(smooth_towards(float(state.redis_memory_used), float(redis_memory_target), 0.3)))
                state.redis_fragmentation_ratio = smooth_towards(
                    state.redis_fragmentation_ratio,
                    clamp(1.05 + redis_miss_ratio * 0.42 + (state.redis_memory_used / max(profile.memory_total, 1)) * 0.12, 1.02, 1.9),
                    0.24,
                )
                state.redis_keys = max(100, int(round(smooth_towards(float(state.redis_keys), float(redis_keys_target), 0.18))))
                state.redis_expires = max(50, int(round(smooth_towards(float(state.redis_expires), float(redis_expires_target), 0.2))))

            if profile.kafka_partitions:
                lag_target = max(2.0, sample.kafka_receives * 8.0 - sample.kafka_processes * 4.0 + sample.kafka_publishes * 2.0)
                lag_target += 26.0 if service_name == "orders-worker" and sample.kafka_receives else 0.0
                lag_sum_target = lag_target * max(profile.kafka_partitions / 2.0, 2.0)
                state.kafka_lag = int(round(smooth_towards(float(state.kafka_lag), lag_target, 0.28)))
                state.kafka_lag_sum = int(round(smooth_towards(float(state.kafka_lag_sum), lag_sum_target, 0.28)))
                state.kafka_assigned = profile.kafka_partitions
                state.kafka_process_ms = smooth_towards(
                    state.kafka_process_ms,
                    max(12.0, 18.0 + sample.kafka_receives * 3.0 + kafka_backlog_pressure * 4.5),
                    0.34,
                )

    def resource_health_metrics(self, timestamp_ns: int) -> list[NumberMetricSpec | HistogramMetricSpec]:
        metrics: list[NumberMetricSpec | HistogramMetricSpec] = []
        uptime = time.time() - self.started_at_unix
        interval = max(float(self.args.interval_sec), 1.0)

        for resource in SERVICE_RESOURCES.values():
            profile = SERVICE_PROFILES[resource.service_name]
            state = self.service_states[resource.service_name]
            cpu_util = round(state.cpu_util, 4)
            mem_util = round(state.mem_util, 4)
            disk_util = round(state.disk_util, 4)
            net_util = round(state.net_util, 4)
            memory_used = state.memory_used
            memory_total = profile.memory_total
            fd_count = state.fd_count

            metrics.extend(
                [
                    self.gauge_metric(resource, "system.cpu.utilization", cpu_util, {"system.cpu.utilization": cpu_util}),
                    self.gauge_metric(resource, "system.cpu.usage", cpu_util * 1000.0, {}),
                    self.gauge_metric(resource, "process.cpu.usage", state.process_cpu_usage, {}),
                    self.gauge_metric(resource, "system.memory.utilization", mem_util, {"system.memory.utilization": mem_util}),
                    self.gauge_metric(resource, "system.memory.usage", memory_used, {"system.memory.state": "used"}),
                    self.gauge_metric(resource, "system.memory.usage", max(memory_total - memory_used, 0), {"system.memory.state": "free"}),
                    self.gauge_metric(resource, "system.disk.utilization", disk_util, {"system.disk.utilization": disk_util}),
                    self.gauge_metric(resource, "disk.free", state.disk_free, {"system.filesystem.mountpoint": "/"}),
                    self.gauge_metric(resource, "disk.total", profile.disk_total, {"system.filesystem.mountpoint": "/"}),
                    self.gauge_metric(resource, "system.network.utilization", net_util, {"system.network.utilization": net_util}),
                    self.gauge_metric(resource, "system.network.io", state.rx_bytes_per_sec, {"system.network.io.direction": "receive"}),
                    self.gauge_metric(resource, "system.network.io", state.tx_bytes_per_sec, {"system.network.io.direction": "transmit"}),
                    self.gauge_metric(resource, "system.network.packets", max(state.rx_bytes_per_sec // 1500, 120), {"system.network.io.direction": "receive"}),
                    self.gauge_metric(resource, "system.network.packets", max(state.tx_bytes_per_sec // 1500, 100), {"system.network.io.direction": "transmit"}),
                    self.gauge_metric(resource, "system.network.errors", 1 if state.net_util > 0.72 else 0, {"system.network.io.direction": "receive"}),
                    self.gauge_metric(resource, "system.network.dropped", 0, {"system.network.io.direction": "transmit"}),
                    self.gauge_metric(resource, "system.network.connections", state.connections, {"system.network.state": "established"}),
                    self.sum_metric(
                        resource,
                        "system.cpu.time",
                        self.bump_counter(f"{resource.service_name}:system_cpu_user", max(state.cpu_util * interval, 0.1)),
                        {"system.cpu.state": "user"},
                        monotonic=True,
                    ),
                    self.sum_metric(
                        resource,
                        "system.cpu.time",
                        self.bump_counter(f"{resource.service_name}:system_cpu_system", max(state.cpu_util * interval * 0.44, 0.05)),
                        {"system.cpu.state": "system"},
                        monotonic=True,
                    ),
                    self.sum_metric(
                        resource,
                        "process.cpu.time",
                        self.bump_counter(f"{resource.service_name}:process_cpu_user", max(state.process_cpu_usage * interval, 0.08)),
                        {"process.cpu.state": "user"},
                        monotonic=True,
                    ),
                    self.gauge_metric(resource, "process.memory.usage", memory_used // 2, {}),
                    self.gauge_metric(resource, "process.memory.virtual", memory_used, {}),
                    self.gauge_metric(resource, "process.open_file_descriptor.count", fd_count, {}),
                    self.gauge_metric(resource, "process.uptime", uptime, {}),
                    self.sum_metric(
                        resource,
                        "container.cpu.time",
                        self.bump_counter(f"{resource.service_name}:container_cpu_time", max(state.process_cpu_usage * interval * 1.15, 0.1)),
                        {"container.name": resource.container_name},
                        monotonic=True,
                    ),
                    self.gauge_metric(resource, "container.memory.usage", memory_used, {"container.name": resource.container_name}),
                    self.gauge_metric(resource, "k8s.container.restarts", state.restarts, {"container.name": resource.container_name}),
                    self.gauge_metric(resource, "k8s.node.allocatable.cpu", 8, {"k8s.node.name": resource.host_name}),
                    self.gauge_metric(resource, "k8s.node.allocatable.memory", 32_000_000_000, {"k8s.node.name": resource.host_name}),
                ]
            )

            metrics.extend(self.jvm_metrics(resource.service_name))

            if profile.db_pool_max:
                metrics.extend(
                    self.db_connection_metrics(
                        resource.service_name,
                        utilization=state.db_active / max(profile.db_pool_max, 1),
                        active=state.db_active,
                        max_conn=profile.db_pool_max,
                    )
                )

        return metrics

    def http_metrics_from_span(self, span: SpanSpec, request_bytes: int, response_bytes: int) -> list[NumberMetricSpec | HistogramMetricSpec]:
        state = self.service_states[span.resource.service_name]
        route = str(span.attributes.get("http.route", ""))
        method = str(span.attributes.get("http.request.method") or span.attributes.get("http.method") or "")
        status = int(span.attributes.get("http.response.status_code", 200))
        duration_ms = max(1.0, ns_to_ms(span.end_unix_nano - span.start_unix_nano))
        attrs = {
            "service.name": span.resource.service_name,
            "http.request.method": method,
            "http.method": method,
            "http.route": route,
            "http.response.status_code": str(status),
        }
        active_requests = max(1, int(round(state.active_requests + self.rng.uniform(-1.0, 1.2))))
        return [
            self.histogram_metric(span.resource, "http.server.request.duration", duration_ms, attrs, bounds=[25, 50, 100, 200, 400, 800]),
            self.gauge_metric(span.resource, "http.server.active_requests", active_requests, attrs),
            self.histogram_metric(span.resource, "http.server.request.body.size", float(request_bytes), attrs, bounds=[256, 512, 1024, 2048, 4096, 8192]),
            self.histogram_metric(span.resource, "http.server.response.body.size", float(response_bytes), attrs, bounds=[512, 1024, 2048, 4096, 8192, 16384]),
            self.histogram_metric(span.resource, "http.client.request.duration", duration_ms * 0.72, attrs, bounds=[10, 25, 50, 100, 200, 400]),
        ]

    def db_metrics_from_span(self, span: SpanSpec) -> list[NumberMetricSpec | HistogramMetricSpec]:
        state = self.service_states[span.resource.service_name]
        attrs = {
            "service.name": span.resource.service_name,
            "db.system": span.attributes.get("db.system", "postgresql"),
            "db.namespace": span.attributes.get("db.namespace", "app"),
            "db.operation.name": span.attributes.get("db.operation.name", "SELECT"),
            "db.response.status_code": span.attributes.get("db.response.status_code", "ok"),
            "server.address": span.attributes.get("server.address", POSTGRES_ADDR),
        }
        duration_ms = max(1.0, ns_to_ms(span.end_unix_nano - span.start_unix_nano))
        return [
            self.histogram_metric(span.resource, "db.client.operation.duration", duration_ms, attrs, bounds=[5, 10, 25, 50, 100, 250, 500]),
            self.gauge_metric(span.resource, "db.client.connection.count", max(state.db_active, 1), attrs),
            self.gauge_metric(span.resource, "db.client.connection.max", max(SERVICE_PROFILES[span.resource.service_name].db_pool_max, 8), attrs),
            self.gauge_metric(span.resource, "db.client.connection.idle.max", max(SERVICE_PROFILES[span.resource.service_name].db_pool_max // 2, 4), attrs),
            self.gauge_metric(span.resource, "db.client.connection.idle.min", max(SERVICE_PROFILES[span.resource.service_name].db_pool_max // 12, 1), attrs),
            self.gauge_metric(span.resource, "db.client.connection.pending_requests", state.db_pending, attrs),
            self.sum_metric(span.resource, "db.client.connection.timeouts", self.bump_counter(f"{span.resource.service_name}:db_timeouts", 0 if span.status_code == STATUS_OK else 1), attrs, monotonic=True, as_int=True),
            self.histogram_metric(span.resource, "db.client.connection.create_time", max(1.0, 2.2 + state.cpu_util * 4.0), attrs, bounds=[1, 2, 5, 10, 20]),
            self.histogram_metric(span.resource, "db.client.connection.wait_time", max(duration_ms * 0.3, state.db_wait_ms), attrs, bounds=[1, 2, 5, 10, 25, 50]),
            self.histogram_metric(span.resource, "db.client.connection.use_time", max(duration_ms * 0.7, state.db_use_ms), attrs, bounds=[1, 5, 10, 25, 50, 100, 250]),
        ]

    def db_connection_metrics(self, service_name: str, utilization: float, active: int, max_conn: int) -> list[NumberMetricSpec]:
        resource = SERVICE_RESOURCES[service_name]
        state = self.service_states[service_name]
        utilization = max(utilization, state.db_active / max(max_conn, 1))
        active = max(active, state.db_active)
        attrs = {"service.name": service_name, "db.connection_pool.utilization": utilization}
        return [
            self.gauge_metric(resource, "db.connection.pool.utilization", utilization, attrs),
            self.gauge_metric(resource, "hikaricp.connections.active", active, {"service.name": service_name, "pool.name": "primary"}),
            self.gauge_metric(resource, "hikaricp.connections.max", max_conn, {"service.name": service_name, "pool.name": "primary"}),
            self.gauge_metric(resource, "jdbc.connections.active", max(active - 1, 0), {"service.name": service_name, "pool.name": "primary"}),
            self.gauge_metric(resource, "jdbc.connections.max", max_conn, {"service.name": service_name, "pool.name": "primary"}),
        ]

    def redis_metrics(self, service_name: str, hits: int, misses: int, memory_used: int, commands: int, evicted: int) -> list[NumberMetricSpec]:
        resource = SERVICE_RESOURCES[service_name]
        state = self.service_states[service_name]
        common = {
            "service.name": service_name,
            "server.address": REDIS_ADDR,
            "redis.db": "0",
        }
        return [
            self.sum_metric(resource, "redis.keyspace.hits", self.bump_counter(f"{service_name}:redis_hits", hits), common, monotonic=True, as_int=True),
            self.sum_metric(resource, "redis.keyspace.misses", self.bump_counter(f"{service_name}:redis_misses", misses), common, monotonic=True, as_int=True),
            self.gauge_metric(resource, "redis.clients.connected", max(state.redis_clients, 8), common),
            self.gauge_metric(resource, "redis.memory.used", max(memory_used, state.redis_memory_used), common),
            self.gauge_metric(resource, "redis.memory.fragmentation_ratio", round(max(state.redis_fragmentation_ratio, 1.02), 4), common),
            self.sum_metric(resource, "redis.commands.processed", self.bump_counter(f"{service_name}:redis_cmd", commands), common, monotonic=True, as_int=True),
            self.sum_metric(resource, "redis.keys.evicted", self.bump_counter(f"{service_name}:redis_evict", evicted), common, monotonic=True, as_int=True),
            self.gauge_metric(resource, "redis.db.keys", max(state.redis_keys, 100), common),
            self.gauge_metric(resource, "redis.db.expires", max(state.redis_expires, 50), common),
        ]

    def kafka_metrics(self, service_name: str, lag: int, lag_sum: int, published: int, received: int, assigned: int) -> list[NumberMetricSpec | HistogramMetricSpec]:
        resource = SERVICE_RESOURCES[service_name]
        state = self.service_states[service_name]
        attrs = {
            "service.name": service_name,
            "messaging.system": "kafka",
            "messaging.destination.name": "orders.events",
            "messaging.consumer.group.name": "orders-worker",
            "server.address": KAFKA_BROKER,
        }
        return [
            self.sum_metric(resource, "messaging.publish.messages", self.bump_counter(f"{service_name}:publish", published), attrs, monotonic=True, as_int=True),
            self.histogram_metric(resource, "messaging.publish.duration", 8 + state.net_util * 18.0, attrs, bounds=[2, 5, 10, 20, 50]),
            self.sum_metric(resource, "messaging.receive.messages", self.bump_counter(f"{service_name}:receive", received), attrs, monotonic=True, as_int=True),
            self.histogram_metric(resource, "messaging.process.duration", max(state.kafka_process_ms, 10.0), attrs, bounds=[5, 10, 25, 50, 100]),
            self.gauge_metric(resource, "messaging.kafka.consumer.lag", max(lag, state.kafka_lag), attrs),
            self.gauge_metric(resource, "messaging.kafka.consumer.lag_sum", max(lag_sum, state.kafka_lag_sum), attrs),
            self.gauge_metric(resource, "messaging.kafka.consumer.assigned_partitions", max(assigned, state.kafka_assigned), attrs),
            self.histogram_metric(resource, "messaging.client.operation.duration", max(10.0, state.kafka_process_ms * 0.72), attrs, bounds=[5, 10, 20, 40, 80]),
        ]

    def ai_metrics(self, llm_span: SpanSpec, model: str) -> list[NumberMetricSpec | HistogramMetricSpec]:
        state = self.service_states[llm_span.resource.service_name]
        attrs = {
            "service.name": llm_span.resource.service_name,
            "server.address": "api.openai.com",
            "gen_ai.request.model": model,
            "gen_ai.operation.name": "chat.completions",
            "gen_ai.conversation.id": llm_span.attributes["gen_ai.conversation.id"],
        }
        duration_ms = max(1.0, ns_to_ms(llm_span.end_unix_nano - llm_span.start_unix_nano))
        return [
            self.histogram_metric(llm_span.resource, "rpc.server.duration", duration_ms, attrs, bounds=[50, 100, 200, 400, 800]),
            self.gauge_metric(llm_span.resource, "process.cpu.usage", state.process_cpu_usage, {"service.name": llm_span.resource.service_name}),
        ]

    def jvm_metrics(self, service_name: str) -> list[NumberMetricSpec | HistogramMetricSpec]:
        resource = SERVICE_RESOURCES[service_name]
        state = self.service_states[service_name]
        profile = SERVICE_PROFILES[service_name]
        interval = max(float(self.args.interval_sec), 1.0)
        
        heap_used = int(state.memory_used * 0.7)
        heap_max = int(profile.memory_total * 0.8)
        non_heap_used = int(state.memory_used * 0.15)
        
        metrics: list[NumberMetricSpec | HistogramMetricSpec] = []
        
        # Memory Pools
        common_attrs = {"service.name": service_name}
        for pool_name, pool_type, used_val in [
            ("G1 Old Generation", "heap", heap_used),
            ("G1 Survivor Space", "heap", int(heap_used * 0.05)),
            ("G1 Eden Space", "heap", int(heap_used * 0.2)),
            ("Metaspace", "non_heap", non_heap_used),
            ("CodeCache", "non_heap", int(non_heap_used * 0.3)),
        ]:
            attrs = {**common_attrs, "jvm.memory.pool.name": pool_name, "jvm.memory.type": pool_type}
            metrics.append(self.gauge_metric(resource, "jvm.memory.used", float(used_val), attrs))
            metrics.append(self.gauge_metric(resource, "jvm.memory.committed", float(used_val * 1.1), attrs))
            limit_val = heap_max if pool_type == "heap" else -1.0
            metrics.append(self.gauge_metric(resource, "jvm.memory.limit", float(limit_val), attrs))

        # GC Metrics
        gc_duration = 5.0 + (state.cpu_util * 45.0)
        gc_count = 1 if self.rng.random() > 0.8 else 0
        for gc_name, gc_action in [("G1 Young Generation", "end of minor GC"), ("G1 Old Generation", "end of major GC")]:
            gc_attrs = {**common_attrs, "jvm.gc.name": gc_name, "jvm.gc.action": gc_action}
            metrics.append(self.histogram_metric(resource, "jvm.gc.duration", gc_duration, gc_attrs, bounds=[10, 50, 100, 200, 500]))
        
        # Thread/Class Metrics
        metrics.extend([
            self.gauge_metric(resource, "jvm.thread.count", float(state.connections + 40), {**common_attrs, "jvm.thread.daemon": "false"}),
            self.gauge_metric(resource, "jvm.thread.count", 15.0, {**common_attrs, "jvm.thread.daemon": "true"}),
            self.gauge_metric(resource, "jvm.class.loaded", 8500.0 + self.loop_index, common_attrs),
            self.gauge_metric(resource, "jvm.class.count", 8450.0, common_attrs),
        ])

        # CPU Metrics
        metrics.extend([
            self.gauge_metric(resource, "jvm.cpu.recent_utilization", state.process_cpu_usage * 0.85, common_attrs),
            self.sum_metric(resource, "jvm.cpu.time", self.bump_counter(f"{service_name}:jvm_cpu_time", state.process_cpu_usage * interval * 0.9), common_attrs, monotonic=True),
        ])

        # Buffers
        for pool_name in ["direct", "mapped"]:
            buf_attrs = {**common_attrs, "jvm.buffer.pool.name": pool_name}
            metrics.append(self.gauge_metric(resource, "jvm.buffer.memory.usage", float(non_heap_used * 0.1), buf_attrs))
            metrics.append(self.gauge_metric(resource, "jvm.buffer.count", 120.0, buf_attrs))

        return metrics

    def export_traces(self, spans: list[SpanSpec], api_key: str) -> None:
        grouped = defaultdict(list)
        for span in spans:
            grouped[span.resource].append(span)

        request = trace_service_pb2.ExportTraceServiceRequest(
            resource_spans=[
                trace_pb2.ResourceSpans(
                    resource=resource_pb2.Resource(attributes=key_values(resource.attributes())),
                    scope_spans=[
                        trace_pb2.ScopeSpans(
                            scope=common_pb2.InstrumentationScope(
                                name=INSTRUMENTATION_SCOPE,
                                version=INSTRUMENTATION_VERSION,
                            ),
                            spans=[self.to_proto_span(span) for span in grouped_spans],
                        )
                    ],
                )
                for resource, grouped_spans in grouped.items()
            ]
        )
        self.trace_stub.Export(request, metadata=[("x-api-key", api_key)])

    def export_logs(self, logs: list[LogSpec], api_key: str) -> None:
        grouped = defaultdict(list)
        for log in logs:
            grouped[log.resource].append(log)

        request = logs_service_pb2.ExportLogsServiceRequest(
            resource_logs=[
                logs_pb2.ResourceLogs(
                    resource=resource_pb2.Resource(attributes=key_values(resource.attributes())),
                    scope_logs=[
                        logs_pb2.ScopeLogs(
                            scope=common_pb2.InstrumentationScope(
                                name=INSTRUMENTATION_SCOPE,
                                version=INSTRUMENTATION_VERSION,
                            ),
                            log_records=[self.to_proto_log(log) for log in grouped_logs],
                        )
                    ],
                )
                for resource, grouped_logs in grouped.items()
            ]
        )
        self.logs_stub.Export(request, metadata=[("x-api-key", api_key)])

    def export_metrics(self, metrics: list[NumberMetricSpec | HistogramMetricSpec], api_key: str) -> None:
        grouped = defaultdict(list)
        for metric in metrics:
            grouped[metric.resource].append(metric)

        request = metrics_service_pb2.ExportMetricsServiceRequest(
            resource_metrics=[
                metrics_pb2.ResourceMetrics(
                    resource=resource_pb2.Resource(attributes=key_values(resource.attributes())),
                    scope_metrics=[
                        metrics_pb2.ScopeMetrics(
                            scope=common_pb2.InstrumentationScope(
                                name=INSTRUMENTATION_SCOPE,
                                version=INSTRUMENTATION_VERSION,
                            ),
                            metrics=[self.to_proto_metric(metric) for metric in grouped_metrics],
                        )
                    ],
                )
                for resource, grouped_metrics in grouped.items()
            ]
        )
        self.metrics_stub.Export(request, metadata=[("x-api-key", api_key)])

    def to_proto_span(self, span: SpanSpec) -> trace_pb2.Span:
        return trace_pb2.Span(
            trace_id=span.trace_id,
            span_id=span.span_id,
            parent_span_id=span.parent_span_id,
            name=span.name,
            kind=span.kind,
            start_time_unix_nano=span.start_unix_nano,
            end_time_unix_nano=span.end_unix_nano,
            attributes=key_values(span.attributes),
            status=trace_pb2.Status(code=span.status_code, message=span.status_message),
            events=[
                trace_pb2.Span.Event(
                    name=event.name,
                    time_unix_nano=event.time_unix_nano,
                    attributes=key_values(event.attributes),
                )
                for event in span.events
            ],
        )

    def to_proto_log(self, log: LogSpec) -> logs_pb2.LogRecord:
        return logs_pb2.LogRecord(
            time_unix_nano=log.timestamp_nano,
            observed_time_unix_nano=log.observed_timestamp_nano,
            severity_text=log.severity_text,
            severity_number=log.severity_number,
            body=any_value(log.body),
            attributes=key_values(log.attributes),
            trace_id=log.trace_id,
            span_id=log.span_id,
            flags=1,
        )

    def to_proto_metric(self, metric: NumberMetricSpec | HistogramMetricSpec) -> metrics_pb2.Metric:
        now_ns = time.time_ns()
        if isinstance(metric, NumberMetricSpec):
            data_point = metrics_pb2.NumberDataPoint(
                attributes=key_values(metric.attributes),
                time_unix_nano=now_ns,
            )
            if metric.as_int:
                data_point.as_int = int(metric.value)
            else:
                data_point.as_double = float(metric.value)

            if metric.monotonic:
                return metrics_pb2.Metric(
                    name=metric.name,
                    description=metric.description,
                    unit=metric.unit,
                    sum=metrics_pb2.Sum(
                        aggregation_temporality=metric.temporality,
                        is_monotonic=True,
                        data_points=[data_point],
                    ),
                )

            return metrics_pb2.Metric(
                name=metric.name,
                description=metric.description,
                unit=metric.unit,
                gauge=metrics_pb2.Gauge(data_points=[data_point]),
            )

        hist_point = metrics_pb2.HistogramDataPoint(
            attributes=key_values(metric.attributes),
            time_unix_nano=now_ns,
            count=metric.count,
            sum=metric.sum_value,
            explicit_bounds=metric.explicit_bounds,
            bucket_counts=metric.bucket_counts,
        )
        return metrics_pb2.Metric(
            name=metric.name,
            description=metric.description,
            unit=metric.unit,
            histogram=metrics_pb2.Histogram(
                aggregation_temporality=metric.temporality,
                data_points=[hist_point],
            ),
        )

    def new_span(
        self,
        resource: ResourceSpec,
        trace_id: bytes,
        parent_hex: str | bytes,
        name: str,
        kind: int,
        start_ns: int,
        end_ns: int,
        attributes: dict[str, Any],
        status_code: int = STATUS_OK,
        status_message: str = "",
        events: list[SpanEventSpec] | None = None,
    ) -> SpanSpec:
        return SpanSpec(
            resource=resource,
            trace_id=trace_id,
            span_id=self.rand_span_id(),
            parent_span_id=parent_hex if isinstance(parent_hex, bytes) else b"",
            name=name,
            kind=kind,
            start_unix_nano=start_ns,
            end_unix_nano=end_ns,
            attributes=attributes,
            status_code=status_code,
            status_message=status_message,
            events=list(events or []),
        )

    def child_client_span(
        self,
        source_resource: ResourceSpec,
        _target_resource: ResourceSpec,
        trace_id: bytes,
        parent_span_id: bytes,
        name: str,
        start_ns: int,
        duration_ms: int,
        method: str,
        route: str,
        http_status_code: int,
        extra_attrs: dict[str, Any],
        otel_status_code: int = STATUS_OK,
        status_message: str = "",
    ) -> SpanSpec:
        attrs = {
            "http.method": method,
            "http.request.method": method,
            "http.route": route,
            "http.url": f"https://demo.optikk.app{route}",
            "http.response.status_code": http_status_code,
        }
        attrs.update(extra_attrs)
        return self.new_span(
            source_resource,
            trace_id,
            parent_span_id,
            name,
            trace_pb2.Span.SPAN_KIND_CLIENT,
            start_ns,
            start_ns + ms_to_ns(duration_ms),
            attrs,
            status_code=otel_status_code,
            status_message=status_message,
        )

    def child_external_client_span(
        self,
        source_resource: ResourceSpec,
        trace_id: bytes,
        parent_span_id: bytes,
        name: str,
        start_ns: int,
        duration_ms: int,
        method: str,
        host: str,
        url_path: str,
        status_code: int,
        otel_status_code: int = STATUS_OK,
        status_message: str = "",
    ) -> SpanSpec:
        attrs = {
            "http.method": method,
            "http.request.method": method,
            "http.host": host,
            "http.url": f"https://{host}{url_path}",
            "http.response.status_code": status_code,
            "server.address": host,
        }
        return self.new_span(
            source_resource,
            trace_id,
            parent_span_id,
            name,
            trace_pb2.Span.SPAN_KIND_CLIENT,
            start_ns,
            start_ns + ms_to_ns(duration_ms),
            attrs,
            status_code=otel_status_code,
            status_message=status_message,
        )

    def mirrored_server_span(
        self,
        resource: ResourceSpec,
        trace_id: bytes,
        parent_span_id: bytes,
        name: str,
        start_ns: int,
        duration_ms: int,
        method: str,
        route: str,
        response_status: int,
        extra_attrs: dict[str, Any],
        status_code: int = STATUS_OK,
        status_message: str = "",
    ) -> SpanSpec:
        attrs = {
            "http.method": method,
            "http.request.method": method,
            "http.route": route,
            "http.response.status_code": response_status,
        }
        attrs.update(extra_attrs)
        return self.new_span(
            resource,
            trace_id,
            parent_span_id,
            name,
            trace_pb2.Span.SPAN_KIND_SERVER,
            start_ns,
            start_ns + ms_to_ns(duration_ms),
            attrs,
            status_code=status_code,
            status_message=status_message,
        )

    def db_span(
        self,
        resource: ResourceSpec,
        trace_id: bytes,
        parent_span_id: bytes,
        name: str,
        start_ns: int,
        duration_ms: int,
        system: str,
        namespace: str,
        operation: str,
        response_status: str,
    ) -> SpanSpec:
        return self.new_span(
            resource,
            trace_id,
            parent_span_id,
            name,
            trace_pb2.Span.SPAN_KIND_CLIENT,
            start_ns,
            start_ns + ms_to_ns(duration_ms),
            {
                "db.system": system,
                "db.namespace": namespace,
                "db.operation.name": operation,
                "db.response.status_code": response_status,
                "server.address": POSTGRES_ADDR,
            },
        )

    def redis_span(
        self,
        resource: ResourceSpec,
        trace_id: bytes,
        parent_span_id: bytes,
        name: str,
        start_ns: int,
        duration_ms: int,
        operation: str,
        hit: bool = True,
    ) -> SpanSpec:
        return self.new_span(
            resource,
            trace_id,
            parent_span_id,
            name,
            trace_pb2.Span.SPAN_KIND_CLIENT,
            start_ns,
            start_ns + ms_to_ns(duration_ms),
            {
                "db.system": "redis",
                "db.operation.name": operation,
                "server.address": REDIS_ADDR,
                "cache.hit": hit,
            },
        )

    def kafka_span(
        self,
        resource: ResourceSpec,
        trace_id: bytes,
        parent_span_id: bytes,
        name: str,
        kind: int,
        start_ns: int,
        duration_ms: int,
        operation: str,
        destination: str,
        consumer_group: str,
    ) -> SpanSpec:
        return self.new_span(
            resource,
            trace_id,
            parent_span_id,
            name,
            kind,
            start_ns,
            start_ns + ms_to_ns(duration_ms),
            {
                "messaging.system": "kafka",
                "messaging.destination.name": destination,
                "messaging.consumer.group.name": consumer_group,
                "messaging.operation.name": operation,
                "server.address": KAFKA_BROKER,
            },
        )

    def make_log(
        self,
        span: SpanSpec,
        timestamp_ns: int,
        severity_text: str,
        severity_number: int,
        body: str,
        attributes: dict[str, Any],
    ) -> LogSpec:
        attrs = {"service.name": span.resource.service_name}
        attrs.update(attributes)
        return LogSpec(
            resource=span.resource,
            trace_id=span.trace_id,
            span_id=span.span_id,
            timestamp_nano=timestamp_ns,
            observed_timestamp_nano=timestamp_ns,
            severity_text=severity_text,
            severity_number=severity_number,
            body=body,
            attributes=attrs,
        )

    def gauge_metric(self, resource: ResourceSpec, name: str, value: float, attrs: dict[str, Any]) -> NumberMetricSpec:
        return NumberMetricSpec(resource=resource, name=name, value=float(value), attributes=attrs, monotonic=False)

    def sum_metric(
        self,
        resource: ResourceSpec,
        name: str,
        value: float,
        attrs: dict[str, Any],
        monotonic: bool,
        as_int: bool = False,
    ) -> NumberMetricSpec:
        return NumberMetricSpec(
            resource=resource,
            name=name,
            value=value,
            attributes=attrs,
            monotonic=monotonic,
            temporality=TEMPORALITY_CUMULATIVE,
            as_int=as_int,
        )

    def histogram_metric(
        self,
        resource: ResourceSpec,
        name: str,
        value: float,
        attrs: dict[str, Any],
        bounds: list[float],
    ) -> HistogramMetricSpec:
        bucket_counts = histogram_counts(value, bounds)
        return HistogramMetricSpec(
            resource=resource,
            name=name,
            count=1,
            sum_value=float(value),
            explicit_bounds=bounds,
            bucket_counts=bucket_counts,
            attributes=attrs,
            temporality=TEMPORALITY_DELTA,
        )

    def rand_trace_id(self) -> bytes:
        return bytes(self.rng.getrandbits(8) for _ in range(16))

    def rand_span_id(self) -> bytes:
        span_id = b""
        while not span_id or span_id == b"\x00" * 8:
            span_id = bytes(self.rng.getrandbits(8) for _ in range(8))
        return span_id

    def rand_token(self, prefix: str) -> str:
        suffix = "".join(self.rng.choice("0123456789abcdef") for _ in range(10))
        return f"{prefix}_{suffix}"

    def bump_counter(self, key: str, delta: float) -> float:
        self.counters[key] += delta
        return self.counters[key]

    def pick_conversation_id(self) -> str:
        keys = sorted(self.ai_conversation_turns)
        return keys[(self.loop_index + self.rng.randint(0, len(keys) - 1)) % len(keys)]


@dataclass
class CycleResult:
    spans: list[SpanSpec]
    logs: list[LogSpec]
    metrics: list[NumberMetricSpec | HistogramMetricSpec]
    scenarios: list[ScenarioResult]


def any_value(value: Any) -> common_pb2.AnyValue:
    if isinstance(value, bool):
        return common_pb2.AnyValue(bool_value=value)
    if isinstance(value, int) and not isinstance(value, bool):
        return common_pb2.AnyValue(int_value=value)
    if isinstance(value, float):
        return common_pb2.AnyValue(double_value=value)
    if isinstance(value, bytes):
        return common_pb2.AnyValue(bytes_value=value)
    return common_pb2.AnyValue(string_value=str(value))


def key_values(values: dict[str, Any]) -> list[common_pb2.KeyValue]:
    return [common_pb2.KeyValue(key=key, value=any_value(value)) for key, value in values.items()]


def histogram_counts(value: float, bounds: Iterable[float]) -> list[int]:
    result = []
    remaining = 1
    placed = False
    for bound in bounds:
        if not placed and value <= bound:
            result.append(1)
            placed = True
            remaining = 0
        else:
            result.append(0)
    result.append(remaining)
    return result


def ms_to_ns(value_ms: float) -> int:
    return int(value_ms * 1_000_000)


def ns_to_ms(value_ns: int) -> float:
    return value_ns / 1_000_000.0


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(value, maximum))


def smooth_towards(current: float, target: float, factor: float) -> float:
    factor = clamp(factor, 0.0, 1.0)
    return current + ((target - current) * factor)


def slugify(value: str) -> str:
    lowered = value.strip().lower()
    chars = []
    last_dash = False
    for ch in lowered:
        if ch.isalnum():
            chars.append(ch)
            last_dash = False
        elif not last_dash:
            chars.append("-")
            last_dash = True
    slug = "".join(chars).strip("-")
    return slug or "demo-team"


def iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def http_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw)
            error_message = payload.get("error", {}).get("message", raw)
        except Exception:
            error_message = raw
        raise RuntimeError(f"HTTP {exc.code} {url}: {error_message}") from exc


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send demo OTLP traces, logs, and metrics to Optikk.")
    parser.add_argument("--backend-http", default="http://localhost:9090")
    parser.add_argument("--backend-grpc", default="localhost:4317")
    parser.add_argument("--state-file", default=STATE_FILE_DEFAULT)
    parser.add_argument("--org-name", default="Optikk Demo Org")
    parser.add_argument("--team-name", default="Website Demo")
    parser.add_argument("--user-name", default="Demo User")
    parser.add_argument("--user-email", default="demo@optikk.local")
    parser.add_argument("--user-password", default="DemoPass123!")
    parser.add_argument("--interval-sec", type=int, default=5)
    parser.add_argument("--traces-per-interval", type=int, default=25)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--api-key")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--reset-team", action="store_true")
    args = parser.parse_args(argv)
    if args.interval_sec <= 0:
        parser.error("--interval-sec must be > 0")
    if args.traces_per_interval <= 0:
        parser.error("--traces-per-interval must be > 0")
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    generator = DemoOTLPGenerator(args)
    try:
        generator.run()
    except KeyboardInterrupt:
        print("Stopped.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
