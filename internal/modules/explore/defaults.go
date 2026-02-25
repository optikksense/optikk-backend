package explore

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("overview", defaultOverview)
	dashboardconfig.RegisterDefaultConfig("error-dashboard", defaultErrorDashboard)
	dashboardconfig.RegisterDefaultConfig("service-detail", defaultServiceDetail)
	dashboardconfig.RegisterDefaultConfig("infrastructure", defaultInfrastructure)
	dashboardconfig.RegisterDefaultConfig("services", defaultServices)
}

const defaultOverview = `page: overview
title: "Overview"
icon: "Activity"
subtitle: "Monitor your system health"

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
    iconColor: "#5E60CE"
    sparklineDataSource: metrics-timeseries
    sparklineValueField: request_count
    sparklineColor: "#5E60CE"
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
    suffix: "%"
  - title: "Avg Latency"
    dataSource: metrics-summary
    valueField: avg_latency
    formatter: duration
    icon: Clock
    iconColor: "#F79009"
    trendInverted: true
    sparklineDataSource: metrics-timeseries
    sparklineValueField: avg_latency
    sparklineColor: "#F79009"
  - title: "P95 Latency"
    dataSource: metrics-summary
    valueField: p95_latency
    formatter: duration
    icon: Zap
    iconColor: "#06AED5"
    trendInverted: true

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

const defaultErrorDashboard = `page: error-dashboard
title: "Error Dashboard"
icon: "AlertCircle"
subtitle: "Error trends, breakdowns, and grouped error logs across all services"

dataSources:
  - id: service-timeseries
    endpoint: /v1/services/timeseries
    params:
      interval: "5m"
  - id: services-metrics
    endpoint: /v1/services/metrics

statCards:
  - title: "Total Errors"
    dataSource: _errorGroups
    valueField: _totalErrors
    formatter: number
    icon: AlertCircle
    iconColor: "#F04438"
  - title: "Affected Services"
    dataSource: _errorGroups
    valueField: _uniqueServices
    icon: Server
    iconColor: "#F79009"
  - title: "Affected Endpoints"
    dataSource: _errorGroups
    valueField: _uniqueOperations
    icon: AlertCircle
    iconColor: "#5E60CE"
  - title: "Top Error Group"
    dataSource: _errorGroups
    valueField: _topErrorCount
    formatter: number
    icon: AlertCircle
    iconColor: "#E478FA"

charts:
  - id: error-rate-trend
    title: "Error Rate Trend by Service (%)"
    type: error-rate
    layout:
      col: 24
    dataSource: service-timeseries
    groupByKey: service
    endpointMetricsSource: services-metrics
    endpointListType: errorRate
`

const defaultServiceDetail = `page: service-detail
title: "Service Detail"
icon: "Activity"

dataSources:
  - id: metrics-timeseries
    endpoint: /v1/metrics/timeseries
    params:
      interval: "5m"
  - id: endpoint-breakdown
    endpoint: /v1/services/{serviceName}/endpoints

charts:
  - id: request-rate
    title: "Request Rate"
    type: request
    layout:
      col: 8
    dataSource: metrics-timeseries
    endpointDataSource: endpoint-breakdown
    endpointListType: requests
    valueKey: request_count
  - id: error-rate
    title: "Error Rate"
    type: error-rate
    layout:
      col: 8
    dataSource: metrics-timeseries
    endpointDataSource: endpoint-breakdown
    endpointListType: errorRate
  - id: latency
    title: "Latency"
    type: latency
    layout:
      col: 8
    dataSource: metrics-timeseries
    endpointDataSource: endpoint-breakdown
    endpointListType: latency
`

const defaultInfrastructure = `page: infrastructure
title: "Infrastructure"
icon: "Server"
subtitle: "Request volume and error rates grouped by host and pod"

dataSources:
  - id: service-timeseries
    endpoint: /v1/services/timeseries
    params:
      interval: "5m"
  - id: services-metrics
    endpoint: /v1/services/metrics

charts:
  - id: request-rate
    title: "Request Rate by Service"
    type: request
    titleIcon: Activity
    layout:
      col: 12
    dataSource: service-timeseries
    groupByKey: service
    valueKey: request_count
    datasetLabel: "Requests"
    height: 240
  - id: error-rate
    title: "Error Rate by Service"
    type: error-rate
    titleIcon: AlertCircle
    layout:
      col: 12
    dataSource: service-timeseries
    groupByKey: service
    endpointMetricsSource: services-metrics
    endpointListType: errorRate
    height: 240
`

const defaultServices = `page: services
title: "Services"
icon: "Layers"
subtitle: "Per-service request rate, error rate, and latency trends"

dataSources:
  - id: service-timeseries
    endpoint: /v1/services/timeseries
    params:
      interval: "5m"
  - id: services-metrics
    endpoint: /v1/services/metrics

charts:
  - id: request-rate
    title: "Request Rate by Service"
    type: request
    titleIcon: Activity
    layout:
      col: 12
    dataSource: service-timeseries
    groupByKey: service
    valueKey: request_count
    datasetLabel: "Requests"
    endpointMetricsSource: services-metrics
    endpointListType: requests
    height: 240
  - id: error-rate
    title: "Error Rate by Service"
    type: error-rate
    titleIcon: AlertCircle
    layout:
      col: 12
    dataSource: service-timeseries
    groupByKey: service
    endpointMetricsSource: services-metrics
    endpointListType: errorRate
    height: 240
  - id: latency
    title: "Avg Latency by Service"
    type: latency
    titleIcon: Clock
    layout:
      col: 24
    dataSource: service-timeseries
    groupByKey: service
    valueKey: avg_latency
    datasetLabel: "Avg Latency"
    height: 240
`
