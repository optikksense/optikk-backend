package overview

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("overview", defaultOverview)
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
