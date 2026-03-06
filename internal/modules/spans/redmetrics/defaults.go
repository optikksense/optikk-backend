package redmetrics

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("metrics", defaultMetrics)
}

const defaultMetrics = `page: metrics
title: "Metrics"
icon: "BarChart2"
subtitle: "RED metrics — request rate, error rate, and duration per service and operation"

tabs:
  - id: red-metrics
    label: RED Metrics
    dataSources:
      - id: service-scorecard
        endpoint: /v1/spans/service-scorecard
      - id: service-timeseries
        endpoint: /v1/services/timeseries
        params:
          interval: "5m"
      - id: top-slow-operations
        endpoint: /v1/spans/top-slow-operations
        params:
          limit: 20
      - id: top-error-operations
        endpoint: /v1/spans/top-error-operations
        params:
          limit: 20
      - id: http-status-distribution
        endpoint: /v1/spans/http-status-distribution
      - id: apdex
        endpoint: /v1/spans/apdex
        params:
          satisfied_ms: 300
          tolerating_ms: 1200
      - id: latency-histogram
        endpoint: /v1/latency/histogram

    statCards:
      - title: "Services"
        dataSource: service-scorecard
        valueField: _count
        formatter: number
        icon: Layers

    charts:
      - id: service-scorecard
        title: "Service Scorecard"
        type: scorecard
        titleIcon: Activity
        layout:
          col: 24
        dataSource: service-scorecard
        height: 160
      - id: request-rate
        title: "Request Rate by Service"
        type: request
        titleIcon: TrendingUp
        layout:
          col: 12
        dataSource: service-timeseries
        groupByKey: service_name
        valueKey: request_count
        height: 260
      - id: error-rate
        title: "Error Rate by Service"
        type: error-rate
        titleIcon: AlertCircle
        layout:
          col: 12
        dataSource: service-timeseries
        groupByKey: service_name
        height: 260
      - id: p95-latency
        title: "p95 Latency by Service"
        type: latency
        titleIcon: Clock
        layout:
          col: 12
        dataSource: service-timeseries
        groupByKey: service_name
        valueKey: p95
        height: 260
      - id: apdex
        title: "Apdex Score by Service"
        type: gauge
        titleIcon: Target
        layout:
          col: 12
        dataSource: apdex
        valueKey: apdex
        groupByKey: serviceName
        height: 260
      - id: http-status-distribution
        title: "HTTP Status Code Distribution"
        type: bar
        titleIcon: BarChart2
        layout:
          col: 12
        dataSource: http-status-distribution
        dataKey: buckets
        groupByKey: statusCode
        valueKey: spanCount
        height: 260
      - id: latency-histogram
        title: "Latency Histogram"
        type: latency-histogram
        titleIcon: BarChart3
        layout:
          col: 12
        dataSource: latency-histogram
        height: 260
      - id: top-slow-operations
        title: "Top Slowest Operations (p99)"
        type: table
        titleIcon: TrendingDown
        layout:
          col: 12
        dataSource: top-slow-operations
        height: 360
      - id: top-error-operations
        title: "Top Error Operations"
        type: table
        titleIcon: AlertTriangle
        layout:
          col: 12
        dataSource: top-error-operations
        height: 360
`
