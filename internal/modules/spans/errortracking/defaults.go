package errortracking

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("overview", defaultOverview)
}

const defaultOverview = `page: overview
title: "Overview"
icon: "LayoutDashboard"
subtitle: "Service health, error tracking, and SLO compliance"

tabs:
  - id: summary
    label: Summary
    dataSources:
      - id: overview-summary
        endpoint: /v1/overview/summary
      - id: overview-timeseries
        endpoint: /v1/overview/timeseries
        params:
          interval: "5m"
      - id: overview-services
        endpoint: /v1/overview/services
      - id: overview-endpoints
        endpoint: /v1/overview/endpoints
    charts:
      - id: request-rate
        title: "Request Rate"
        type: request
        titleIcon: TrendingUp
        layout:
          col: 12
        dataSource: overview-timeseries
        groupByKey: service_name
        height: 260
      - id: error-rate
        title: "Error Rate"
        type: error-rate
        titleIcon: AlertCircle
        layout:
          col: 12
        dataSource: overview-timeseries
        groupByKey: service_name
        height: 260
      - id: p95-latency
        title: "p95 Latency"
        type: latency
        titleIcon: Clock
        layout:
          col: 24
        dataSource: overview-timeseries
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
        dataSource: overview-endpoints
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
