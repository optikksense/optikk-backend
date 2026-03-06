package overview

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("overview", defaultOverview)
}

// The overview page config is owned by the overview module. The metrics page
// continues to be registered by the redmetrics module; the constant below is
// kept only as a legacy reference.
const defaultOverview = `page: overview
title: "Overview"
icon: "LayoutDashboard"
subtitle: "Service health, error tracking, and SLO compliance"

tabs:
  - id: summary
    label: Summary
    dataSources:
      - id: overview-request-rate
        endpoint: /v1/overview/request-rate
      - id: overview-error-rate
        endpoint: /v1/overview/error-rate
      - id: overview-p95-latency
        endpoint: /v1/overview/p95-latency
      - id: overview-services
        endpoint: /v1/overview/services
      - id: overview-top-endpoints
        endpoint: /v1/overview/top-endpoints
    charts:
      - id: request-rate
        title: "Request Rate"
        type: request
        titleIcon: TrendingUp
        layout:
          col: 12
        dataSource: overview-request-rate
        groupByKey: service_name
        height: 260
      - id: error-rate
        title: "Error Rate"
        type: error-rate
        titleIcon: AlertCircle
        layout:
          col: 12
        dataSource: overview-error-rate
        groupByKey: service_name
        height: 260
      - id: p95-latency
        title: "p95 Latency"
        type: latency
        titleIcon: Clock
        layout:
          col: 24
        dataSource: overview-p95-latency
        groupByKey: service_name
        valueKey: p95
        height: 260
      - id: services-table
        title: "Services"
        type: table
        titleIcon: Layers
        layout:
          col: 12
        dataSource: overview-services
        height: 360
      - id: endpoints-table
        title: "Top Endpoints"
        type: table
        titleIcon: List
        layout:
          col: 12
        dataSource: overview-top-endpoints
        height: 360

  - id: errors
    label: Errors
    dataSources:
      - id: exception-rate-by-type
        endpoint: /v1/spans/exception-rate-by-type
      - id: error-hotspot
        endpoint: /v1/spans/error-hotspot
      - id: http-5xx-by-route
        endpoint: /v1/spans/http-5xx-by-route
      - id: error-groups
        endpoint: /v1/errors/groups
      - id: error-timeseries
        endpoint: /v1/errors/timeseries
      - id: service-error-rate
        endpoint: /v1/overview/errors/service-error-rate
    charts:
      - id: service-error-rate
        title: "Service Error Rate"
        type: error-rate
        titleIcon: AlertCircle
        layout:
          col: 12
        dataSource: service-error-rate
        groupByKey: service
        height: 260
      - id: exception-rate-by-type
        title: "Exception Rate by Type"
        type: area
        titleIcon: Bug
        layout:
          col: 12
        dataSource: exception-rate-by-type
        groupByKey: exceptionType
        valueKey: count
        height: 260
      - id: error-hotspot
        title: "Error Hotspot (Service × Operation)"
        type: heatmap
        titleIcon: Crosshair
        layout:
          col: 24
        dataSource: error-hotspot
        xKey: operationName
        yKey: serviceName
        valueKey: errorRate
        height: 360
      - id: http-5xx-by-route
        title: "HTTP 5xx by Route"
        type: bar
        titleIcon: AlertTriangle
        layout:
          col: 12
        dataSource: http-5xx-by-route
        groupByKey: httpRoute
        valueKey: count
        height: 260
      - id: error-groups
        title: "Error Groups"
        type: table
        titleIcon: List
        layout:
          col: 12
        dataSource: error-groups
        height: 360

  - id: slo
    label: SLO
    dataSources:
      - id: slo-sli-insights
        endpoint: /v1/overview/slo
        params:
          interval: "5m"
    charts:
      - id: availability
        title: "Availability Over Time"
        type: error-rate
        titleIcon: Target
        layout:
          col: 8
        dataSource: slo-sli-insights
        dataKey: timeseries
        valueField: availability_percent
        targetThreshold: 99.9
        datasetLabel: "Availability %"
        color: "#12B76A"
        height: 220
      - id: latency-vs-target
        title: "Latency vs Target"
        type: latency
        titleIcon: Clock
        layout:
          col: 8
        dataSource: slo-sli-insights
        dataKey: timeseries
        valueField: avg_latency_ms
        targetThreshold: 300
        height: 220
      - id: error-budget-burn
        title: "Error Budget Burn"
        type: error-rate
        titleIcon: Flame
        layout:
          col: 8
        dataSource: slo-sli-insights
        dataKey: timeseries
        valueField: _errorBudgetBurn
        targetThreshold: 0.1
        height: 220
`

const defaultMetrics = `page: metrics
title: "Metrics"
icon: "BarChart3"
subtitle: "System-wide performance metrics"

dataSources:
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
