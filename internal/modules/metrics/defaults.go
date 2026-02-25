package metrics

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("metrics", defaultMetrics)
}

const defaultMetrics = `page: metrics
title: "Metrics"
icon: "BarChart3"
subtitle: "System-wide performance metrics"

dataSources:
  - id: metrics-summary
    endpoint: /v1/metrics/summary
  - id: metrics-timeseries
    endpoint: /v1/metrics/timeseries
    params:
      interval: "5m"
  - id: endpoints-timeseries
    endpoint: /v1/endpoints/timeseries
  - id: endpoints-metrics
    endpoint: /v1/endpoints/metrics

statCards:
  - title: "Total Requests"
    dataSource: metrics-summary
    valueField: total_requests
    formatter: number
    icon: Activity
    iconColor: "#3B82F6"
    sparklineDataSource: metrics-timeseries
    sparklineValueField: request_count
    sparklineColor: "#3B82F6"
  - title: "Error Rate"
    dataSource: metrics-summary
    valueField: error_rate
    formatter: percent
    icon: AlertCircle
    iconColor: "#F04438"
    trendInverted: true
    sparklineDataSource: metrics-timeseries
    sparklineValueField: error_rate
    sparklineColor: "#F04438"
  - title: "Avg Latency"
    dataSource: metrics-summary
    valueField: avg_latency
    formatter: latencyMs
    icon: Clock
    iconColor: "#10B981"
    sparklineDataSource: metrics-timeseries
    sparklineValueField: avg_latency
    sparklineColor: "#10B981"
  - title: "P95 Latency"
    dataSource: metrics-summary
    valueField: p95_latency
    formatter: latencyMs
    icon: Clock
    iconColor: "#F59E0B"

charts:
  - id: request-rate
    title: "Request Rate"
    type: request
    layout:
      col: 12
    dataSource: metrics-timeseries
    endpointDataSource: endpoints-timeseries
    endpointMetricsSource: endpoints-metrics
    endpointListType: requests
    valueKey: request_count
  - id: error-rate
    title: "Error Rate"
    type: error-rate
    layout:
      col: 12
    dataSource: metrics-timeseries
    endpointDataSource: endpoints-timeseries
    endpointMetricsSource: endpoints-metrics
    endpointListType: errorRate
  - id: latency-distribution
    title: "Latency Distribution"
    type: latency
    layout:
      col: 24
    dataSource: metrics-timeseries
    endpointDataSource: endpoints-timeseries
    endpointMetricsSource: endpoints-metrics
    endpointListType: latency
`
