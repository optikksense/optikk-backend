package overview

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("overview", defaultOverview)
	dashboardconfig.RegisterDefaultConfig("metrics", defaultMetrics)
}

const defaultOverview = `page: overview
title: "Overview"
icon: "Activity"
subtitle: "Monitor your system health"

dataSources:
  - id: metrics-summary
    endpoint: /v1/overview/summary
  - id: metrics-timeseries
    endpoint: /v1/overview/timeseries
    params:
      interval: "5m"
  - id: endpoints-timeseries
    endpoint: /v1/overview/endpoints/timeseries
  - id: endpoints-metrics
    endpoint: /v1/overview/endpoints/metrics
  - id: overview-services
    endpoint: /v1/overview/services

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
  - id: http-request-rate
    endpoint: /v1/http/request-rate
  - id: http-request-duration
    endpoint: /v1/http/request-duration
  - id: http-active-requests
    endpoint: /v1/http/active-requests
  - id: http-client-duration
    endpoint: /v1/http/client-duration
  - id: dns-tls-duration
    endpoint: /v1/http/dns-duration

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
  - id: http-request-rate
    title: "HTTP Request Rate by Status Code"
    type: request
    titleIcon: Globe
    layout:
      col: 12
    dataSource: http-request-rate
    groupByKey: status_code
    valueKey: count
    height: 260
  - id: http-request-duration
    title: "HTTP Server Request Duration p50/p95/p99"
    type: stat
    titleIcon: Clock
    layout:
      col: 12
    dataSource: http-request-duration
    height: 120
  - id: http-active-requests
    title: "Active HTTP Requests"
    type: area
    titleIcon: Activity
    layout:
      col: 12
    dataSource: http-active-requests
    valueKey: value
    height: 260
  - id: http-client-duration
    title: "Outbound HTTP Duration p50/p95/p99"
    type: stat
    titleIcon: ArrowUpRight
    layout:
      col: 12
    dataSource: http-client-duration
    height: 120
  - id: dns-tls-duration
    title: "DNS Lookup & TLS Handshake Duration"
    type: stat
    titleIcon: Shield
    layout:
      col: 24
    dataSource: dns-tls-duration
    height: 120
`
