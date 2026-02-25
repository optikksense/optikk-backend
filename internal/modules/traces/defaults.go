package traces

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("traces", defaultTraces)
	dashboardconfig.RegisterDefaultConfig("latency-analysis", defaultLatencyAnalysis)
}

const defaultTraces = `page: traces
title: "Traces"
icon: "GitBranch"
subtitle: "Distributed trace analysis — latency and error trends"

dataSources:
  - id: metrics-timeseries
    endpoint: /v1/metrics/timeseries
    params:
      interval: "5m"
  - id: endpoints-timeseries
    endpoint: /v1/endpoints/timeseries
  - id: endpoints-metrics
    endpoint: /v1/endpoints/metrics
  - id: latency-histogram
    endpoint: /v1/latency/histogram

charts:
  - id: trace-duration-distribution
    title: "Trace Duration Distribution"
    type: latency-histogram
    titleIcon: Clock
    layout:
      col: 12
    dataSource: latency-histogram
    height: 160
  - id: latency-distribution
    title: "Latency Distribution (Timeseries)"
    type: latency
    titleIcon: Clock
    layout:
      col: 12
    dataSource: metrics-timeseries
    endpointDataSource: endpoints-timeseries
    endpointMetricsSource: endpoints-metrics
    endpointListType: latency
    height: 240
  - id: error-rate
    title: "Error Rate"
    type: error-rate
    titleIcon: AlertCircle
    layout:
      col: 24
    dataSource: metrics-timeseries
    endpointDataSource: endpoints-timeseries
    endpointMetricsSource: endpoints-metrics
    endpointListType: errorRate
    height: 240
`

const defaultLatencyAnalysis = `page: latency-analysis
title: "Latency Analysis"
icon: "Clock"
subtitle: "Latency histogram and heatmap for trace duration analysis"

dataSources:
  - id: latency-histogram
    endpoint: /v1/latency/histogram
  - id: latency-heatmap
    endpoint: /v1/latency/heatmap
    params:
      interval: "5m"

charts:
  - id: histogram
    title: "Latency Histogram"
    type: latency-histogram
    titleIcon: BarChart3
    layout:
      col: 24
    dataSource: latency-histogram
    height: 200
  - id: heatmap
    title: "Latency Heatmap"
    type: latency-heatmap
    titleIcon: Activity
    layout:
      col: 24
    dataSource: latency-heatmap
`
