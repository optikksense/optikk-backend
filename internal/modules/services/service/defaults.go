package servicepage

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("services", defaultServices)
}

const defaultServices = `page: services
title: "Services"
icon: "Layers"
subtitle: "Global service health and request trends"

dataSources:
  - id: services-metrics
    endpoint: /v1/services/metrics
  - id: service-timeseries
    endpoint: /v1/services/timeseries
    params:
      interval: "5m"

charts:
  - id: services-request-rate
    title: "Service Request Rate"
    type: request
    layout:
      col: 12
    dataSource: service-timeseries
    groupByKey: service
  - id: services-error-rate
    title: "Service Error Rate"
    type: error-rate
    layout:
      col: 12
    dataSource: service-timeseries
    groupByKey: service
  - id: services-latency
    title: "Service Latency"
    type: latency
    layout:
      col: 24
    dataSource: service-timeseries
    groupByKey: service
`
