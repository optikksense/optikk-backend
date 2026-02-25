package infrastructure

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("infrastructure", defaultInfrastructure)
}

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
    endpointMetricsSource: services-metrics
    endpointListType: requests
    listTitle: "Services"
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
