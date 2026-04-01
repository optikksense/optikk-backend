package dashboardcfg

// Code generated from dashboard tab configs; DO NOT EDIT.
var dashboardEndpointFieldContracts = map[string]map[string]struct{}{
	"/v1/ai/cost/metrics": {
		"model_name":     {},
		"total_cost_usd": {},
	},
	"/v1/ai/cost/timeseries": {
		"cost_per_interval": {},
		"model_name":        {},
	},
	"/v1/ai/cost/token-breakdown": {
		"cache_tokens":      {},
		"completion_tokens": {},
		"model_name":        {},
		"prompt_tokens":     {},
		"system_tokens":     {},
	},
	"/v1/ai/performance/latency-histogram": {
		"bucket_ms":     {},
		"model_name":    {},
		"request_count": {},
	},
	"/v1/ai/performance/metrics": {
		"model_name": {},
	},
	"/v1/ai/performance/timeseries": {
		"avg_latency_ms": {},
		"model_name":     {},
		"qps":            {},
		"tokens_per_sec": {},
	},
	"/v1/ai/security/metrics": {
		"model_name": {},
	},
	"/v1/ai/security/pii-categories": {
		"model_name": {},
	},
	"/v1/ai/security/timeseries": {
		"guardrail_count": {},
		"model_name":      {},
		"pii_count":       {},
	},
	"/v1/ai/summary": {
		"guardrail_count": {},
		"pii_count":       {},
		"request_count":   {},
		"total_cost_usd":  {},
	},
	"/v1/apm/messaging-publish-duration": {},
	"/v1/apm/open-fds": {
		"value": {},
	},
	"/v1/apm/process-cpu": {
		"state": {},
		"value": {},
	},
	"/v1/apm/process-memory": {
		"rss": {},
		"vms": {},
	},
	"/v1/apm/rpc-duration": {
		"p95": {},
		"p99": {},
	},
	"/v1/apm/rpc-request-rate": {
		"value": {},
	},
	"/v1/apm/uptime": {
		"value": {},
	},
	"/v1/saturation/database/connections/count": {
		"count": {},
		"state": {},
	},
	"/v1/saturation/database/connections/pending": {
		"count": {},
	},
	"/v1/saturation/database/connections/timeout-rate": {
		"timeout_rate": {},
	},
	"/v1/saturation/database/errors/by-error-type": {
		"errors_per_sec": {},
		"group_by":       {},
	},
	"/v1/saturation/database/errors/by-system": {
		"errors_per_sec": {},
		"group_by":       {},
	},
	"/v1/saturation/database/latency/by-operation": {
		"group_by": {},
		"p99_ms":   {},
	},
	"/v1/saturation/database/latency/by-system": {
		"group_by": {},
		"p99_ms":   {},
	},
	"/v1/saturation/database/ops/by-operation": {
		"group_by":    {},
		"ops_per_sec": {},
	},
	"/v1/saturation/database/ops/by-system": {
		"group_by":    {},
		"ops_per_sec": {},
	},
	"/v1/saturation/database/ops/read-vs-write": {
		"read_ops_per_sec": {},
	},
	"/v1/saturation/database/slow-queries/collections": {},
	"/v1/saturation/database/slow-queries/patterns":    {},
	"/v1/saturation/database/summary": {
		"avg_latency_ms": {},
		"p95_latency_ms": {},
		"p99_latency_ms": {},
		"span_count":     {},
	},
	"/v1/saturation/database/systems": {
		"db_system": {},
	},
	"/v1/errors/groups": {
		"group_id": {},
	},
	"/v1/errors/groups/{errorGroupId}":            {},
	"/v1/errors/groups/{errorGroupId}/timeseries": {},
	"/v1/errors/groups/{errorGroupId}/traces":     {},
	"/v1/http/active-requests": {
		"value": {},
	},
	"/v1/http/client-duration": {},
	"/v1/http/dns-duration":    {},
	"/v1/http/error-timeseries": {
		"error_rate": {},
	},
	"/v1/http/external/error-rate": {
		"error_pct": {},
		"host":      {},
	},
	"/v1/http/external/host-latency": {
		"host":   {},
		"p95_ms": {},
	},
	"/v1/http/external/top-hosts": {
		"host":      {},
		"req_count": {},
	},
	"/v1/http/request-body-size": {},
	"/v1/http/request-duration":  {},
	"/v1/http/request-rate": {
		"count":       {},
		"status_code": {},
	},
	"/v1/http/response-body-size": {},
	"/v1/http/routes/error-rate": {
		"error_pct": {},
		"route":     {},
	},
	"/v1/http/routes/error-timeseries": {
		"error_count": {},
		"http_route":  {},
	},
	"/v1/http/routes/top-by-latency": {
		"p95_ms": {},
		"route":  {},
	},
	"/v1/http/routes/top-by-volume": {
		"req_count": {},
		"route":     {},
	},
	"/v1/http/status-distribution": {
		"count":        {},
		"status_group": {},
	},
	"/v1/infrastructure/cpu/load-average": {
		"load_15m": {},
		"load_1m":  {},
		"load_5m":  {},
	},
	"/v1/infrastructure/cpu/process-count": {
		"state": {},
		"value": {},
	},
	"/v1/infrastructure/cpu/time": {
		"state": {},
		"value": {},
	},
	"/v1/infrastructure/cpu/usage-percentage": {
		"pod":   {},
		"value": {},
	},
	"/v1/infrastructure/disk/filesystem-usage": {
		"mountpoint": {},
		"value":      {},
	},
	"/v1/infrastructure/disk/filesystem-utilization": {
		"pod":   {},
		"value": {},
	},
	"/v1/infrastructure/disk/io": {
		"direction": {},
		"value":     {},
	},
	"/v1/infrastructure/disk/io-time": {
		"pod":   {},
		"value": {},
	},
	"/v1/infrastructure/disk/operations": {
		"direction": {},
		"value":     {},
	},
	"/v1/infrastructure/jvm/buffers": {
		"memory_usage": {},
		"pool_name":    {},
	},
	"/v1/infrastructure/jvm/classes": {
		"loaded": {},
	},
	"/v1/infrastructure/jvm/cpu": {
		"cpu_time_value":     {},
		"recent_utilization": {},
	},
	"/v1/infrastructure/jvm/gc-collections": {
		"collector": {},
		"value":     {},
	},
	"/v1/infrastructure/jvm/gc-duration": {
		"p95": {},
		"p99": {},
	},
	"/v1/infrastructure/jvm/memory": {
		"pool_name": {},
		"used":      {},
	},
	"/v1/infrastructure/jvm/threads": {
		"daemon": {},
		"value":  {},
	},
	"/v1/infrastructure/kubernetes/container-cpu": {
		"container": {},
		"value":     {},
	},
	"/v1/infrastructure/kubernetes/container-memory": {
		"container": {},
		"value":     {},
	},
	"/v1/infrastructure/kubernetes/cpu-throttling": {
		"container": {},
		"value":     {},
	},
	"/v1/infrastructure/kubernetes/node-allocatable": {
		"cpu_cores":    {},
		"memory_bytes": {},
	},
	"/v1/infrastructure/kubernetes/oom-kills": {
		"container": {},
		"value":     {},
	},
	"/v1/infrastructure/kubernetes/pod-phases": {
		"count": {},
		"phase": {},
	},
	"/v1/infrastructure/kubernetes/pod-restarts":   {},
	"/v1/infrastructure/kubernetes/replica-status": {},
	"/v1/infrastructure/kubernetes/volume-usage":   {},
	"/v1/infrastructure/memory/swap": {
		"state": {},
		"value": {},
	},
	"/v1/infrastructure/memory/usage": {
		"state": {},
		"value": {},
	},
	"/v1/infrastructure/memory/usage-percentage": {
		"pod":   {},
		"value": {},
	},
	"/v1/infrastructure/network/connections": {
		"state": {},
		"value": {},
	},
	"/v1/infrastructure/network/dropped": {
		"pod":   {},
		"value": {},
	},
	"/v1/infrastructure/network/errors": {
		"state": {},
		"value": {},
	},
	"/v1/infrastructure/network/io": {
		"direction": {},
		"value":     {},
	},
	"/v1/infrastructure/network/packets": {
		"direction": {},
		"value":     {},
	},
	"/v1/infrastructure/nodes": {
		"host": {},
	},
	"/v1/infrastructure/nodes/summary": {
		"degraded_nodes":  {},
		"healthy_nodes":   {},
		"total_pods":      {},
		"unhealthy_nodes": {},
	},
	"/v1/infrastructure/nodes/{host}/services": {},
	"/v1/infrastructure/resource-utilisation/avg-conn-pool": {
		"value": {},
	},
	"/v1/infrastructure/resource-utilisation/avg-cpu": {
		"value": {},
	},
	"/v1/infrastructure/resource-utilisation/avg-memory": {
		"value": {},
	},
	"/v1/infrastructure/resource-utilisation/avg-network": {
		"value": {},
	},
	"/v1/infrastructure/resource-utilisation/by-instance": {},
	"/v1/infrastructure/resource-utilisation/by-service":  {},
	"/v1/latency/heatmap": {
		"latency_bucket": {},
		"span_count":     {},
		"time_bucket":    {},
	},
	"/v1/latency/histogram": {
		"bucket_label": {},
		"span_count":   {},
	},
	"/v1/logs":           {},
	"/v1/logs/histogram": {},
	"/v1/overview/error-rate": {
		"service_name": {},
	},
	"/v1/overview/errors/service-error-rate": {
		"service": {},
	},
	"/v1/overview/p95-latency": {
		"p95":          {},
		"service_name": {},
	},
	"/v1/overview/request-rate": {
		"service_name": {},
	},
	"/v1/overview/services": {},
	"/v1/overview/slo": {
		"_errorBudgetBurn":     {},
		"availability_percent": {},
		"avg_latency_ms":       {},
	},
	"/v1/overview/slo/stats": {
		"availability_percent": {},
		"error_count":          {},
		"p95_latency_ms":       {},
		"total_requests":       {},
	},
	"/v1/overview/top-endpoints": {},
	"/v1/saturation/kafka/assigned-partitions": {
		"assigned_partitions": {},
	},
	"/v1/saturation/kafka/broker-connections": {
		"broker":      {},
		"connections": {},
	},
	"/v1/saturation/kafka/client-op-duration": {
		"operation_name": {},
		"p95_ms":         {},
	},
	"/v1/saturation/kafka/consume-errors": {
		"error_rate": {},
		"error_type": {},
	},
	"/v1/saturation/kafka/consume-rate-by-group": {
		"consumer_group": {},
		"rate_per_sec":   {},
	},
	"/v1/saturation/kafka/consume-rate-by-topic": {
		"rate_per_sec": {},
		"topic":        {},
	},
	"/v1/saturation/kafka/e2e-latency": {
		"process_p95_ms": {},
		"publish_p95_ms": {},
		"receive_p95_ms": {},
		"topic":          {},
	},
	"/v1/saturation/kafka/lag-by-group": {
		"consumer_group": {},
		"lag":            {},
	},
	"/v1/saturation/kafka/lag-per-partition": {
		"consumer_group": {},
	},
	"/v1/saturation/kafka/process-errors": {
		"error_rate": {},
		"error_type": {},
	},
	"/v1/saturation/kafka/process-rate-by-group": {
		"consumer_group": {},
		"rate_per_sec":   {},
	},
	"/v1/saturation/kafka/produce-rate-by-topic": {
		"rate_per_sec": {},
		"topic":        {},
	},
	"/v1/saturation/kafka/publish-errors": {
		"error_rate": {},
		"error_type": {},
	},
	"/v1/saturation/kafka/publish-latency-by-topic": {
		"p95_ms": {},
		"topic":  {},
	},
	"/v1/saturation/kafka/rebalance-signals": {
		"assigned_partitions":   {},
		"consumer_group":        {},
		"failed_heartbeat_rate": {},
		"rebalance_rate":        {},
	},
	"/v1/saturation/kafka/receive-latency-by-topic": {
		"p95_ms": {},
		"topic":  {},
	},
	"/v1/saturation/kafka/summary-stats": {
		"max_lag":              {},
		"publish_p95_ms":       {},
		"publish_rate_per_sec": {},
		"receive_rate_per_sec": {},
	},
	"/v1/saturation/redis/cache-hit-rate": {
		"hit_rate":     {},
		"hit_rate_pct": {},
		"hits":         {},
		"misses":       {},
	},
	"/v1/saturation/redis/clients": {
		"value": {},
	},
	"/v1/saturation/redis/commands": {
		"value": {},
	},
	"/v1/saturation/redis/evictions": {
		"value": {},
	},
	"/v1/saturation/redis/instances": {
		"instance": {},
	},
	"/v1/saturation/redis/key-expiries": {},
	"/v1/saturation/redis/keyspace":     {},
	"/v1/saturation/redis/memory": {
		"value": {},
	},
	"/v1/saturation/redis/memory-fragmentation": {
		"value": {},
	},
	"/v1/saturation/redis/replication-lag": {
		"offset": {},
	},
	"/v1/services/external-dependencies": {},
	"/v1/services/stats":                 {},
	"/v1/services/timeseries": {
		"p95":     {},
		"service": {},
	},
	"/v1/services/topology": {},
	"/v1/services/{serviceName}/endpoints": {
		"operation_name": {},
		"service_name":   {},
	},
	"/v1/services/{serviceName}/upstream-downstream": {},
	"/v1/spans/client-server-latency":                {},
	"/v1/spans/error-hotspot": {
		"error_rate":     {},
		"operation_name": {},
		"service_name":   {},
	},
	"/v1/spans/exception-rate-by-type": {
		"count":         {},
		"exceptionType": {},
	},
	"/v1/spans/http-5xx-by-route": {
		"count":      {},
		"http_route": {},
	},
	"/v1/spans/latency-breakdown": {
		"serviceName":  {},
		"service_name": {},
		"totalMs":      {},
	},
	"/v1/spans/red/apdex": {
		"apdex":        {},
		"service_name": {},
	},
	"/v1/spans/red/error-rate": {
		"error_pct":    {},
		"service_name": {},
	},
	"/v1/spans/red/errors-by-route": {
		"error_count": {},
		"http_route":  {},
	},
	"/v1/spans/red/p95-latency": {
		"p95":          {},
		"p95_ms":       {},
		"service_name": {},
	},
	"/v1/spans/red/request-rate": {
		"rps":          {},
		"service_name": {},
	},
	"/v1/spans/red/span-kind-breakdown": {
		"kind_string": {},
		"span_count":  {},
	},
	"/v1/spans/red/summary": {
		"avg_error_pct":    {},
		"avg_p50_ms":       {},
		"avg_p95_ms":       {},
		"avg_p99_ms":       {},
		"service_count":    {},
		"total_rps":        {},
		"total_span_count": {},
	},
	"/v1/spans/red/top-error-operations": {},
	"/v1/spans/red/top-slow-operations": {
		"operation_name": {},
		"service_name":   {},
	},
	"/v1/traces": {},
}

func endpointContractHasField(endpoint, field string) bool {
	fields, ok := dashboardEndpointFieldContracts[endpoint]
	if !ok {
		return false
	}
	_, ok = fields[field]
	return ok
}
