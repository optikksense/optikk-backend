package insights

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("messaging-queue", defaultMessagingQueue)
	dashboardconfig.RegisterDefaultConfig("slo-sli", defaultSloSli)
	dashboardconfig.RegisterDefaultConfig("resource-utilization", defaultResourceUtilization)
	dashboardconfig.RegisterDefaultConfig("database-cache", defaultDatabaseCache)
}

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

const defaultSloSli = `page: slo-sli
title: "SLO / SLI Dashboard"
icon: "Target"
subtitle: "Service Level Objectives — availability targets, error budgets, and historical compliance"

dataSources:
  - id: slo-sli-insights
    endpoint: /v1/insights/slo-sli
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

const defaultResourceUtilization = `page: resource-utilization
title: "Resource Utilization"
icon: "Cpu"
subtitle: "CPU, memory, disk, network and connection pool utilization by service/instance"

dataSources:
  - id: resource-utilization
    endpoint: /v1/insights/resource-utilization

statCards:
  - title: "Avg CPU"
    dataSource: resource-utilization
    valueField: _avgCpu
    formatter: percent1
    icon: Cpu
  - title: "Avg Memory"
    dataSource: resource-utilization
    valueField: _avgMemory
    formatter: percent1
    icon: HardDrive
  - title: "Avg Network"
    dataSource: resource-utilization
    valueField: _avgNetwork
    formatter: percent1
    icon: Network
  - title: "Avg Conn Pool"
    dataSource: resource-utilization
    valueField: _avgConnPool
    formatter: percent1
    icon: Database

charts:
  - id: cpu-usage
    title: "CPU Usage Percentage"
    type: request
    titleIcon: Cpu
    layout:
      col: 12
    dataSource: resource-utilization
    dataKey: timeseries
    groupByKey: pod
    valueKey: avg_cpu_util
    datasetLabel: "CPU Util"
    height: 260
  - id: memory-usage
    title: "Memory Usage Percentage"
    type: request
    titleIcon: HardDrive
    layout:
      col: 12
    dataSource: resource-utilization
    dataKey: timeseries
    groupByKey: pod
    valueKey: avg_memory_util
    datasetLabel: "Mem Util"
    height: 260
`

const defaultDatabaseCache = `page: database-cache
title: "Database & Cache Performance"
icon: "Database"
subtitle: "Query latency, cache hit ratio, slow logs, replication lag"

dataSources:
  - id: database-latency-summary
    endpoint: /v1/saturation/database/latency-summary
`
