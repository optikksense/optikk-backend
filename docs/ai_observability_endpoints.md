# AI Observability Endpoints

- Covers `internal/defaultconfig/pages/ai-observability/tabs/overview.json`, `performance.json`, `cost.json`, and `security.json`.
- OTLP ingestion uses gRPC on `4317`.
- Dashboard APIs use `Authorization: Bearer <JWT_TOKEN>` and `X-Team-Id`.
- The current AI repositories read some backend-compat attribute keys such as `gen.ai.*` and `ai.cache_hit`, so the sample includes both those keys and the standard `gen_ai.*` style where useful.

## 1. Shared Variables

```bash
API_BASE="http://localhost:9090"
OTLP_GRPC="localhost:4317"

TOKEN="<JWT_TOKEN>"
TEAM_ID="<TEAM_ID>"
OTLP_API_KEY="<YOUR_API_KEY>"

AUTH_HEADER="Authorization: Bearer ${TOKEN}"
TEAM_HEADER="X-Team-Id: ${TEAM_ID}"

END_MS=$(($(date +%s) * 1000))
START_MS=$((END_MS - 3600000))
NOW_NS=$((END_MS * 1000000))

AI_NS_ONE=$((NOW_NS - 32 * 60 * 1000000000))
AI_NS_TWO=$((NOW_NS - 18 * 60 * 1000000000))
AI_NS_THREE=$((NOW_NS - 7 * 60 * 1000000000))
```

## 2. OTLP gRPC Ingestion Example For AI Dashboards

Each data point below represents one AI request-shaped metric row with all dashboard fields carried in attributes.

```bash
grpcurl -plaintext \
  -H "x-api-key: ${OTLP_API_KEY}" \
  -d @ \
  "${OTLP_GRPC}" \
  opentelemetry.proto.collector.metrics.v1.MetricsService/Export <<EOF
{
  "resourceMetrics": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "ai-gateway" } },
          { "key": "deployment.environment", "value": { "stringValue": "production" } }
        ]
      },
      "scopeMetrics": [
        {
          "scope": { "name": "ai-gateway-metrics", "version": "1.0.0" },
          "metrics": [
            {
              "name": "gen_ai.client.operation.count",
              "unit": "{request}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": false,
                "dataPoints": [
                  {
                    "timeUnixNano": "${AI_NS_ONE}",
                    "asDouble": 1,
                    "attributes": [
                      { "key": "gen_ai.request.model", "value": { "stringValue": "gpt-4o-mini" } },
                      { "key": "gen.ai.request.model", "value": { "stringValue": "gpt-4o-mini" } },
                      { "key": "gen_ai.operation.name", "value": { "stringValue": "chat" } },
                      { "key": "gen.ai.operation.name", "value": { "stringValue": "chat" } },
                      { "key": "server.address", "value": { "stringValue": "openai" } },
                      { "key": "duration_ms", "value": { "doubleValue": 420.0 } },
                      { "key": "gen_ai.usage.input_tokens", "value": { "intValue": "640" } },
                      { "key": "gen.ai.usage.input_tokens", "value": { "intValue": "640" } },
                      { "key": "gen_ai.usage.output_tokens", "value": { "intValue": "220" } },
                      { "key": "gen.ai.usage.output_tokens", "value": { "intValue": "220" } },
                      { "key": "gen_ai.usage.cache_read_input_tokens", "value": { "intValue": "0" } },
                      { "key": "gen.ai.usage.cache_read_input_tokens", "value": { "intValue": "0" } },
                      { "key": "ai.cost_usd", "value": { "doubleValue": 0.0048 } },
                      { "key": "ai.cache_hit", "value": { "intValue": "0" } },
                      { "key": "ai.retry_count", "value": { "intValue": "0" } },
                      { "key": "ai.timeout", "value": { "intValue": "0" } },
                      { "key": "ai.security.pii_detected", "value": { "intValue": "0" } },
                      { "key": "ai.security.guardrail_blocked", "value": { "intValue": "0" } },
                      { "key": "ai.security.content_policy", "value": { "intValue": "0" } },
                      { "key": "error", "value": { "boolValue": false } }
                    ]
                  },
                  {
                    "timeUnixNano": "${AI_NS_TWO}",
                    "asDouble": 1,
                    "attributes": [
                      { "key": "gen_ai.request.model", "value": { "stringValue": "claude-3-5-sonnet" } },
                      { "key": "gen.ai.request.model", "value": { "stringValue": "claude-3-5-sonnet" } },
                      { "key": "gen_ai.operation.name", "value": { "stringValue": "chat" } },
                      { "key": "gen.ai.operation.name", "value": { "stringValue": "chat" } },
                      { "key": "server.address", "value": { "stringValue": "anthropic" } },
                      { "key": "duration_ms", "value": { "doubleValue": 910.0 } },
                      { "key": "gen_ai.usage.input_tokens", "value": { "intValue": "980" } },
                      { "key": "gen.ai.usage.input_tokens", "value": { "intValue": "980" } },
                      { "key": "gen_ai.usage.output_tokens", "value": { "intValue": "310" } },
                      { "key": "gen.ai.usage.output_tokens", "value": { "intValue": "310" } },
                      { "key": "gen_ai.usage.cache_read_input_tokens", "value": { "intValue": "120" } },
                      { "key": "gen.ai.usage.cache_read_input_tokens", "value": { "intValue": "120" } },
                      { "key": "ai.cost_usd", "value": { "doubleValue": 0.0114 } },
                      { "key": "ai.cache_hit", "value": { "intValue": "1" } },
                      { "key": "ai.retry_count", "value": { "intValue": "1" } },
                      { "key": "ai.timeout", "value": { "intValue": "0" } },
                      { "key": "ai.security.pii_detected", "value": { "intValue": "1" } },
                      { "key": "ai.security.guardrail_blocked", "value": { "intValue": "0" } },
                      { "key": "ai.security.content_policy", "value": { "intValue": "1" } },
                      { "key": "error", "value": { "boolValue": false } }
                    ]
                  },
                  {
                    "timeUnixNano": "${AI_NS_THREE}",
                    "asDouble": 1,
                    "attributes": [
                      { "key": "gen_ai.request.model", "value": { "stringValue": "gpt-4.1" } },
                      { "key": "gen.ai.request.model", "value": { "stringValue": "gpt-4.1" } },
                      { "key": "gen_ai.operation.name", "value": { "stringValue": "chat" } },
                      { "key": "gen.ai.operation.name", "value": { "stringValue": "chat" } },
                      { "key": "server.address", "value": { "stringValue": "openai" } },
                      { "key": "duration_ms", "value": { "doubleValue": 1600.0 } },
                      { "key": "gen_ai.usage.input_tokens", "value": { "intValue": "1200" } },
                      { "key": "gen.ai.usage.input_tokens", "value": { "intValue": "1200" } },
                      { "key": "gen_ai.usage.output_tokens", "value": { "intValue": "0" } },
                      { "key": "gen.ai.usage.output_tokens", "value": { "intValue": "0" } },
                      { "key": "gen_ai.usage.cache_read_input_tokens", "value": { "intValue": "0" } },
                      { "key": "gen.ai.usage.cache_read_input_tokens", "value": { "intValue": "0" } },
                      { "key": "ai.cost_usd", "value": { "doubleValue": 0.0062 } },
                      { "key": "ai.cache_hit", "value": { "intValue": "0" } },
                      { "key": "ai.retry_count", "value": { "intValue": "2" } },
                      { "key": "ai.timeout", "value": { "intValue": "1" } },
                      { "key": "ai.security.pii_detected", "value": { "intValue": "0" } },
                      { "key": "ai.security.guardrail_blocked", "value": { "intValue": "1" } },
                      { "key": "ai.security.content_policy", "value": { "intValue": "0" } },
                      { "key": "error", "value": { "boolValue": true } }
                    ]
                  }
                ]
              }
            }
          ]
        }
      ]
    }
  ]
}
EOF
```

## 3. Dashboard API Curls

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/ai/summary"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" --data-urlencode "interval=5m" "${API_BASE}/api/v1/ai/performance/timeseries"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" --data-urlencode "modelName=gpt-4o-mini" "${API_BASE}/api/v1/ai/performance/latency-histogram"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/ai/performance/metrics"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" --data-urlencode "interval=5m" "${API_BASE}/api/v1/ai/cost/timeseries"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/ai/cost/metrics"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/ai/cost/token-breakdown"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" --data-urlencode "interval=5m" "${API_BASE}/api/v1/ai/security/timeseries"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/ai/security/metrics"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/ai/security/pii-categories"
```
