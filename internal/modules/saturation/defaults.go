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
  - id: kafka-queue-lag
    endpoint: /v1/saturation/kafka/queue-lag
  - id: kafka-production-rate
    endpoint: /v1/saturation/kafka/production-rate
  - id: kafka-consumption-rate
    endpoint: /v1/saturation/kafka/consumption-rate
  - id: database-query-table
    endpoint: /v1/saturation/database/query-by-table
  - id: database-avg-latency
    endpoint: /v1/saturation/database/avg-latency

statCards:
  - title: "Avg DB Latency"
    dataSource: database-avg-latency
    valueField: avg_latency_ms
    formatter: number
    icon: Database
  - title: "Max Consumer Lag"
    dataSource: kafka-queue-lag
    valueField: max_consumer_lag
    formatter: number
    icon: Radio
  - title: "Max Queue Depth"
    dataSource: kafka-queue-lag
    valueField: max_queue_depth
    formatter: number
    icon: GitPullRequest

charts:
  - id: consumer-lag
    title: "Consumer Lag"
    type: request
    layout:
      col: 12
    dataSource: kafka-queue-lag
    groupByKey: queue
    valueKey: avg_consumer_lag
    datasetLabel: "Lag"
    listTitle: "Queues"
    height: 220
  - id: database-latencies
    title: "Database Latencies"
    type: request
    layout:
      col: 12
    dataSource: database-avg-latency
    groupByKey: cluster
    valueKey: avg_latency_ms
    datasetLabel: "Avg Latency"
    listTitle: "Database Regions"
    height: 220
`
