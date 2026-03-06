package tracedetail

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("trace-detail", defaultTraceDetail)
}

const defaultTraceDetail = `page: trace-detail
title: "Trace Detail"
icon: "GitBranch"
subtitle: "Full distributed trace — waterfall, span events, critical path, and error chain"

tabs:
  - id: waterfall
    label: Waterfall
    dataSources:
      - id: trace-spans
        endpoint: /v1/traces/{traceId}/spans
      - id: span-events
        endpoint: /v1/traces/{traceId}/span-events
      - id: span-kind-breakdown
        endpoint: /v1/traces/{traceId}/span-kind-breakdown
      - id: span-self-times
        endpoint: /v1/traces/{traceId}/span-self-times
    charts:
      - id: waterfall
        title: "Trace Waterfall"
        type: trace-waterfall
        titleIcon: GitBranch
        layout:
          col: 24
        dataSource: trace-spans
        height: 600
      - id: span-kind-breakdown
        title: "Duration by Span Kind"
        type: bar
        titleIcon: BarChart3
        layout:
          col: 12
        dataSource: span-kind-breakdown
        valueKey: totalDurationMs
        groupByKey: spanKind
        height: 260
      - id: span-self-times
        title: "Self Time vs Total Time"
        type: table
        titleIcon: Clock
        layout:
          col: 12
        dataSource: span-self-times
        height: 260

  - id: errors
    label: Errors
    dataSources:
      - id: span-events
        endpoint: /v1/traces/{traceId}/span-events
      - id: error-path
        endpoint: /v1/traces/{traceId}/error-path
    charts:
      - id: span-events
        title: "Span Events (Exceptions)"
        type: table
        titleIcon: AlertCircle
        layout:
          col: 24
        dataSource: span-events
        height: 320
      - id: error-path
        title: "Error Propagation Path"
        type: trace-waterfall
        titleIcon: AlertTriangle
        layout:
          col: 24
        dataSource: error-path
        height: 320

  - id: performance
    label: Performance
    dataSources:
      - id: critical-path
        endpoint: /v1/traces/{traceId}/critical-path
      - id: span-kind-breakdown
        endpoint: /v1/traces/{traceId}/span-kind-breakdown
    charts:
      - id: critical-path
        title: "Critical Path"
        type: table
        titleIcon: Zap
        layout:
          col: 12
        dataSource: critical-path
        height: 320
      - id: span-kind-chart
        title: "Span Kind Breakdown"
        type: bar
        titleIcon: BarChart3
        layout:
          col: 12
        dataSource: span-kind-breakdown
        valueKey: pctOfTrace
        groupByKey: spanKind
        height: 320
`
