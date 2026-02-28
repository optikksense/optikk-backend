package errors

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("error-dashboard", defaultErrorDashboard)
}

const defaultErrorDashboard = `page: error-dashboard
title: "Error Dashboard"
icon: "AlertCircle"
subtitle: "Service error-rate and error-volume trends"

dataSources:
  - id: service-error-rate
    endpoint: /v1/overview/errors/service-error-rate
  - id: error-volume
    endpoint: /v1/overview/errors/error-volume
  - id: latency-during-error-windows
    endpoint: /v1/overview/errors/latency-during-error-windows
  - id: error-groups
    endpoint: /v1/overview/errors/groups

charts:
  - id: service-error-rate
    title: "Service Error Rate"
    type: error-rate
    layout:
      col: 12
    dataSource: service-error-rate
    groupByKey: service
  - id: service-error-volume
    title: "Error Volume"
    type: request
    layout:
      col: 12
    dataSource: error-volume
    groupByKey: service
    valueKey: error_count
    datasetLabel: "Errors/min"
  - id: service-latency-under-errors
    title: "Latency During Error Windows"
    type: latency
    layout:
      col: 24
    dataSource: latency-during-error-windows
    groupByKey: service
`
