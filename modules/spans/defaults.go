package traces

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("traces", defaultTraces)
	dashboardconfig.RegisterDefaultConfig("latency-analysis", defaultLatencyAnalysis)
	dashboardconfig.RegisterDefaultConfig("error-dashboard", defaultErrorDashboard)
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

const defaultErrorDashboard = `page: error-dashboard
title: "Error Dashboard"
icon: "AlertCircle"
subtitle: "Service error-rate and error-volume trends"

charts:
  - id: service-error-rate
    title: "Service Error Rate"
    type: error-rate
    layout:
      col: 12
    dataSource: service-timeseries
    groupByKey: service
  - id: service-error-volume
    title: "Error Volume"
    type: request
    layout:
      col: 12
    dataSource: service-timeseries
    groupByKey: service
    valueKey: error_count
    datasetLabel: "Errors/min"
  - id: service-latency-under-errors
    title: "Latency During Error Windows"
    type: latency
    layout:
      col: 24
    dataSource: service-timeseries
    groupByKey: service
`
