package saturation

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("saturation", defaultSaturation)
	dashboardconfig.RegisterDefaultConfig("messaging-queue", defaultMessagingQueue)
	dashboardconfig.RegisterDefaultConfig("database-cache", defaultDatabaseCache)
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
  - id: database-latency-summary
    endpoint: /v1/saturation/database/latency-summary
  - id: database-systems
    endpoint: /v1/saturation/database/systems
  - id: database-top-tables
    endpoint: /v1/saturation/database/top-tables
  - id: queue-consumer-lag
    endpoint: /v1/saturation/queue/consumer-lag
  - id: queue-topic-lag
    endpoint: /v1/saturation/queue/topic-lag
  - id: queue-top-queues
    endpoint: /v1/saturation/queue/top-queues

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

const defaultMessagingQueue = `page: messaging-queue
title: "Messaging / Queue Monitoring"
icon: "Network"
subtitle: "Throughput rates, consumer lag, queue depth, and processing errors per queue"

dataSources:
  - id: queue-consumer-lag
    endpoint: /v1/saturation/queue/consumer-lag
  - id: queue-topic-lag
    endpoint: /v1/saturation/queue/topic-lag
  - id: kafka-production-rate
    endpoint: /v1/saturation/kafka/production-rate
  - id: kafka-consumption-rate
    endpoint: /v1/saturation/kafka/consumption-rate

charts:
  - id: production-rate
    title: "Production Rate (msg/s)"
    type: request
    titleIcon: ArrowUpRight
    layout:
      col: 12
    dataSource: kafka-production-rate
    groupByKey: queue
    valueKey: avg_publish_rate
    listType: productionRate
    listTitle: "Production Rate"
    listSortField: avg_publish_rate
  - id: consumption-rate
    title: "Consumption Rate (msg/s)"
    type: request
    titleIcon: ArrowDownRight
    layout:
      col: 12
    dataSource: kafka-consumption-rate
    groupByKey: queue
    valueKey: avg_receive_rate
    listType: consumptionRate
    listTitle: "Consumption Rate"
    listSortField: avg_receive_rate
  - id: consumer-lag
    title: "Consumer Group Lag"
    type: request
    titleIcon: Clock
    layout:
      col: 12
    dataSource: queue-consumer-lag
    groupByKey: queue
    valueKey: avg_consumer_lag
    listType: consumerLag
    listTitle: "Max Lag"
    listSortField: max_consumer_lag
  - id: queue-depth
    title: "Topic Lag (Queue Depth)"
    type: request
    titleIcon: Layers
    layout:
      col: 12
    dataSource: queue-topic-lag
    groupByKey: queue
    valueKey: avg_queue_depth
    listType: depth
    listTitle: "Avg Depth"
    listSortField: avg_queue_depth
`

const defaultDatabaseCache = `page: database-cache
title: "Database & Cache Performance"
icon: "Database"
subtitle: "Query latency, cache hit ratio, slow logs, replication lag"

dataSources:
  - id: database-query-table
    endpoint: /v1/saturation/database/query-by-table
  - id: database-avg-latency
    endpoint: /v1/saturation/database/avg-latency

charts:
  - id: db-query-volume
    title: "Query Volume by Collection"
    type: request
    layout:
      col: 12
    dataSource: database-query-table
    groupByKey: table
    valueKey: query_count
    listTitle: "Query Volume"
    listSortField: query_count
  - id: db-query-latency
    title: "Query Latency by Collection"
    type: latency
    layout:
      col: 12
    dataSource: database-query-table
    groupByKey: table
    valueKey: avg_latency_ms
    listTitle: "Average Latency"
    listSortField: avg_latency_ms
  - id: db-overall-latency
    title: "Overall Database Latency"
    type: latency
    layout:
      col: 24
    dataSource: database-avg-latency
    valueKey: avg_latency_ms
    datasetLabel: "Avg Latency"
`
