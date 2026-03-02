package logs

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("logs", defaultLogs)
}

const defaultLogs = `page: logs
title: "Logs"
icon: "Layers"
subtitle: "Log volume and level distribution over time"

dataSources:
  - id: log-histogram
    endpoint: /v1/logs/histogram
    params:
      interval: "1m"

charts:
  - id: log-volume
    title: "Log Volume by Level"
    type: log-histogram
    titleIcon: Layers
    layout:
      col: 24
    dataSource: log-histogram
    height: 80
`
