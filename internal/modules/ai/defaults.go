package ai

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("ai-observability", defaultAiObservability)
}

const defaultAiObservability = `page: ai-observability
title: "AI Observability"
icon: "Brain"
subtitle: "Performance, cost, and security visibility for LLM / AI model calls"

dataSources:
  - id: ai-perf-ts
    endpoint: /v1/ai/performance/timeseries
    params:
      interval: "5m"
  - id: ai-cost-ts
    endpoint: /v1/ai/cost/timeseries
    params:
      interval: "5m"
  - id: ai-sec-ts
    endpoint: /v1/ai/security/timeseries
    params:
      interval: "5m"
  - id: ai-lat-hist
    endpoint: /v1/ai/performance/latency-histogram
  - id: ai-cost-metrics
    endpoint: /v1/ai/cost/metrics
  - id: ai-token-breakdown
    endpoint: /v1/ai/cost/token-breakdown

charts:
  - id: qps
    title: "QPS per Model"
    type: ai-line
    titleIcon: TrendingDown
    layout:
      col: 12
    dataSource: ai-perf-ts
    groupByKey: model_name
    valueKey: qps
    height: 220
  - id: avg-latency
    title: "Avg Latency ms per Model"
    type: ai-line
    titleIcon: Clock
    layout:
      col: 12
    dataSource: ai-perf-ts
    groupByKey: model_name
    valueKey: avg_latency_ms
    height: 220
  - id: tokens-per-sec
    title: "Tokens / sec per Model"
    type: ai-line
    titleIcon: Zap
    layout:
      col: 12
    dataSource: ai-perf-ts
    groupByKey: model_name
    valueKey: tokens_per_sec
    height: 220
  - id: cost-over-time
    title: "Cost per Interval (USD)"
    type: ai-line
    titleIcon: Activity
    layout:
      col: 12
    dataSource: ai-cost-ts
    groupByKey: model_name
    valueKey: cost_per_interval
    yPrefix: "$"
    yDecimals: 4
    height: 220
  - id: pii-detections
    title: "PII Detections per Model"
    type: ai-line
    titleIcon: ShieldCheck
    layout:
      col: 12
    dataSource: ai-sec-ts
    groupByKey: model_name
    valueKey: pii_count
    height: 220
  - id: guardrail-blocks
    title: "Guardrail Blocks per Model"
    type: ai-line
    titleIcon: AlertCircle
    layout:
      col: 12
    dataSource: ai-sec-ts
    groupByKey: model_name
    valueKey: guardrail_count
    height: 220
  - id: latency-histogram
    title: "Latency Distribution (100ms buckets)"
    type: ai-bar
    titleIcon: BarChart3
    layout:
      col: 12
    dataSource: ai-lat-hist
    groupByKey: model_name
    bucketKey: bucket_ms
    valueKey: request_count
    height: 220
  - id: cost-by-model
    title: "Total Cost by Model (USD)"
    type: ai-bar
    titleIcon: Activity
    layout:
      col: 12
    dataSource: ai-cost-metrics
    labelKey: model_name
    valueKey: total_cost_usd
    color: "#F79009"
    yPrefix: "$"
    yDecimals: 4
    height: 220
  - id: token-breakdown
    title: "Token Breakdown by Model"
    type: ai-bar
    titleIcon: Zap
    layout:
      col: 24
    dataSource: ai-token-breakdown
    labelKey: model_name
    stacked: true
    valueKeys:
      - prompt_tokens
      - completion_tokens
      - system_tokens
      - cache_tokens
    height: 260
`
