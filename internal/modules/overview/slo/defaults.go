package slo

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("slo-sli", defaultSloSli)
}

const defaultSloSli = `page: slo-sli
title: "SLO / SLI Dashboard"
icon: "Target"
subtitle: "Service Level Objectives — availability targets, error budgets, and historical compliance"

dataSources:
  - id: slo-sli-insights
    endpoint: /v1/overview/slo
    params:
      interval: "5m"

charts:
  - id: availability
    title: "Availability Over Time"
    type: error-rate
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
    layout:
      col: 8
    dataSource: slo-sli-insights
    dataKey: timeseries
    valueField: _errorBudgetBurn
    targetThreshold: 0.1
    height: 220
`
