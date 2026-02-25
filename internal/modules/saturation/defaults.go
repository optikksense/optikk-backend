package saturation

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("saturation", defaultSaturation)
}

const defaultSaturation = `page: saturation
title: "Saturation Metrics"
icon: "Gauge"
subtitle: "Leading indicators: queue depths, consumer lag, thread pools, and connection pool utilization"

dataSources:
  - id: saturation-metrics
    endpoint: /v1/saturation/metrics
  - id: saturation-timeseries
    endpoint: /v1/saturation/timeseries
    params:
      interval: "5m"
  - id: services-metrics
    endpoint: /v1/services/metrics

statCards:
  - title: "Max DB Pool Util"
    dataSource: saturation-metrics
    valueField: _maxDbPool
    formatter: percent1
    icon: Database
  - title: "Max Consumer Lag"
    dataSource: saturation-metrics
    valueField: _maxLag
    formatter: number
    icon: Radio
  - title: "Max Thread Active"
    dataSource: saturation-metrics
    valueField: _maxThread
    formatter: number
    icon: Cpu
  - title: "Max Queue Depth"
    dataSource: saturation-metrics
    valueField: _maxQueue
    formatter: number
    icon: GitPullRequest

charts:
  - id: consumer-lag
    title: "Consumer Lag (avg, per service)"
    type: request
    layout:
      col: 12
    dataSource: saturation-timeseries
    groupByKey: service
    valueKey: avg_consumer_lag
    datasetLabel: "Lag"
    endpointMetricsSource: services-metrics
    endpointListType: requests
    listTitle: "Services"
    height: 220
  - id: thread-pool
    title: "Thread Pool Active (avg, per service)"
    type: request
    layout:
      col: 12
    dataSource: saturation-timeseries
    groupByKey: service
    valueKey: avg_thread_active
    datasetLabel: "Active Threads"
    endpointMetricsSource: services-metrics
    endpointListType: requests
    listTitle: "Services"
    height: 220
`
