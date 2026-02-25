package dashboardconfig

// DefaultConfigs maps pageId to its default YAML chart configuration.
var DefaultConfigs = map[string]string{
	"overview":             defaultOverview,
	"metrics":              defaultMetrics,
	"messaging-queue":      defaultMessagingQueue,
	"saturation":           defaultSaturation,
	"error-dashboard":      defaultErrorDashboard,
	"slo-sli":              defaultSloSli,
	"service-detail":       defaultServiceDetail,
	"resource-utilization": defaultResourceUtilization,
	"logs":                 defaultLogs,
	"traces":               defaultTraces,
	"infrastructure":       defaultInfrastructure,
	"services":             defaultServices,
	"ai-observability":     defaultAiObservability,
	"latency-analysis":     defaultLatencyAnalysis,
}

const defaultOverview = `page: overview
title: "Overview"
icon: "Activity"
subtitle: "Monitor your system health"

dataSources:
  - id: metrics-summary
    endpoint: /v1/metrics/summary
  - id: metrics-timeseries
    endpoint: /v1/metrics/timeseries
    params:
      interval: "5m"
  - id: endpoints-timeseries
    endpoint: /v1/endpoints/timeseries
  - id: endpoints-metrics
    endpoint: /v1/endpoints/metrics

statCards:
  - title: "Total Requests"
    dataSource: metrics-summary
    valueField: total_requests
    formatter: number
    icon: Activity
    iconColor: "#5E60CE"
    sparklineDataSource: metrics-timeseries
    sparklineValueField: request_count
    sparklineColor: "#5E60CE"
  - title: "Error Rate"
    dataSource: metrics-summary
    valueField: error_rate
    formatter: percent
    icon: AlertCircle
    iconColor: "#F04438"
    trendInverted: true
    sparklineDataSource: metrics-timeseries
    sparklineValueField: error_rate
    sparklineColor: "#F04438"
    suffix: "%"
  - title: "Avg Latency"
    dataSource: metrics-summary
    valueField: avg_latency
    formatter: duration
    icon: Clock
    iconColor: "#F79009"
    trendInverted: true
    sparklineDataSource: metrics-timeseries
    sparklineValueField: avg_latency
    sparklineColor: "#F79009"
  - title: "P95 Latency"
    dataSource: metrics-summary
    valueField: p95_latency
    formatter: duration
    icon: Zap
    iconColor: "#06AED5"
    trendInverted: true

charts:
  - id: request-rate
    title: "Request Rate"
    type: request
    layout:
      col: 12
    dataSource: metrics-timeseries
    endpointDataSource: endpoints-timeseries
    endpointMetricsSource: endpoints-metrics
    endpointListType: requests
    valueKey: request_count
  - id: error-rate
    title: "Error Rate"
    type: error-rate
    layout:
      col: 12
    dataSource: metrics-timeseries
    endpointDataSource: endpoints-timeseries
    endpointMetricsSource: endpoints-metrics
    endpointListType: errorRate
  - id: latency-distribution
    title: "Latency Distribution"
    type: latency
    layout:
      col: 24
    dataSource: metrics-timeseries
    endpointDataSource: endpoints-timeseries
    endpointMetricsSource: endpoints-metrics
    endpointListType: latency
`

const defaultMetrics = `page: metrics
title: "Metrics"
icon: "BarChart3"
subtitle: "System-wide performance metrics"

dataSources:
  - id: metrics-summary
    endpoint: /v1/metrics/summary
  - id: metrics-timeseries
    endpoint: /v1/metrics/timeseries
    params:
      interval: "5m"
  - id: endpoints-timeseries
    endpoint: /v1/endpoints/timeseries
  - id: endpoints-metrics
    endpoint: /v1/endpoints/metrics

statCards:
  - title: "Total Requests"
    dataSource: metrics-summary
    valueField: total_requests
    formatter: number
    icon: Activity
    iconColor: "#3B82F6"
    sparklineDataSource: metrics-timeseries
    sparklineValueField: request_count
    sparklineColor: "#3B82F6"
  - title: "Error Rate"
    dataSource: metrics-summary
    valueField: error_rate
    formatter: percent
    icon: AlertCircle
    iconColor: "#F04438"
    trendInverted: true
    sparklineDataSource: metrics-timeseries
    sparklineValueField: error_rate
    sparklineColor: "#F04438"
  - title: "Avg Latency"
    dataSource: metrics-summary
    valueField: avg_latency
    formatter: latencyMs
    icon: Clock
    iconColor: "#10B981"
    sparklineDataSource: metrics-timeseries
    sparklineValueField: avg_latency
    sparklineColor: "#10B981"
  - title: "P95 Latency"
    dataSource: metrics-summary
    valueField: p95_latency
    formatter: latencyMs
    icon: Clock
    iconColor: "#F59E0B"

charts:
  - id: request-rate
    title: "Request Rate"
    type: request
    layout:
      col: 12
    dataSource: metrics-timeseries
    endpointDataSource: endpoints-timeseries
    endpointMetricsSource: endpoints-metrics
    endpointListType: requests
    valueKey: request_count
  - id: error-rate
    title: "Error Rate"
    type: error-rate
    layout:
      col: 12
    dataSource: metrics-timeseries
    endpointDataSource: endpoints-timeseries
    endpointMetricsSource: endpoints-metrics
    endpointListType: errorRate
  - id: latency-distribution
    title: "Latency Distribution"
    type: latency
    layout:
      col: 24
    dataSource: metrics-timeseries
    endpointDataSource: endpoints-timeseries
    endpointMetricsSource: endpoints-metrics
    endpointListType: latency
`

const defaultMessagingQueue = `page: messaging-queue
title: "Messaging / Queue Monitoring"
icon: "Network"
subtitle: "Throughput rates, consumer lag, queue depth, and processing errors per queue"

dataSources:
  - id: messaging-queue-insights
    endpoint: /v1/insights/messaging-queue
    params:
      interval: "5m"

statCards:
  - title: "Avg Queue Depth"
    dataSource: messaging-queue-insights
    valueField: summary.avg_queue_depth
    formatter: fixed1
    icon: Layers
  - title: "Max Consumer Lag"
    dataSource: messaging-queue-insights
    valueField: summary.max_consumer_lag
    formatter: fixed1
    icon: Clock
  - title: "Avg Processing Errors"
    dataSource: messaging-queue-insights
    valueField: summary.processing_errors
    formatter: fixed0
    icon: AlertTriangle
  - title: "Total Queues"
    dataSource: messaging-queue-insights
    valueField: _uniqueQueues
    icon: Network

charts:
  - id: production-rate
    title: "Production Rate (msg/s)"
    type: request
    titleIcon: ArrowUpRight
    layout:
      col: 12
    dataSource: messaging-queue-insights
    dataKey: timeseries
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
    dataSource: messaging-queue-insights
    dataKey: timeseries
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
    dataSource: messaging-queue-insights
    dataKey: timeseries
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
    dataSource: messaging-queue-insights
    dataKey: timeseries
    groupByKey: queue
    valueKey: avg_queue_depth
    listType: depth
    listTitle: "Avg Depth"
    listSortField: avg_queue_depth
`

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
    height: 220
  - id: queue-depth
    title: "Queue Depth (avg, per service)"
    type: request
    layout:
      col: 12
    dataSource: saturation-timeseries
    groupByKey: service
    valueKey: avg_queue_depth
    datasetLabel: "Queue Depth"
    height: 220
`

const defaultErrorDashboard = `page: error-dashboard
title: "Error Dashboard"
icon: "AlertCircle"
subtitle: "Error trends, breakdowns, and grouped error logs across all services"

dataSources:
  - id: service-timeseries
    endpoint: /v1/services/timeseries
    params:
      interval: "5m"
  - id: services-metrics
    endpoint: /v1/services/metrics

statCards:
  - title: "Total Errors"
    dataSource: _errorGroups
    valueField: _totalErrors
    formatter: number
    icon: AlertCircle
    iconColor: "#F04438"
  - title: "Affected Services"
    dataSource: _errorGroups
    valueField: _uniqueServices
    icon: Server
    iconColor: "#F79009"
  - title: "Affected Endpoints"
    dataSource: _errorGroups
    valueField: _uniqueOperations
    icon: AlertCircle
    iconColor: "#5E60CE"
  - title: "Top Error Group"
    dataSource: _errorGroups
    valueField: _topErrorCount
    formatter: number
    icon: AlertCircle
    iconColor: "#E478FA"

charts:
  - id: error-rate-trend
    title: "Error Rate Trend by Service (%)"
    type: error-rate
    layout:
      col: 24
    dataSource: service-timeseries
    groupByKey: service
    endpointMetricsSource: services-metrics
    endpointListType: errorRate
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

const defaultServiceDetail = `page: service-detail
title: "Service Detail"
icon: "Activity"

dataSources:
  - id: metrics-timeseries
    endpoint: /v1/metrics/timeseries
    params:
      interval: "5m"
  - id: endpoint-breakdown
    endpoint: /v1/services/{serviceName}/endpoints

charts:
  - id: request-rate
    title: "Request Rate"
    type: request
    layout:
      col: 8
    dataSource: metrics-timeseries
    endpointDataSource: endpoint-breakdown
    endpointListType: requests
    valueKey: request_count
  - id: error-rate
    title: "Error Rate"
    type: error-rate
    layout:
      col: 8
    dataSource: metrics-timeseries
    endpointDataSource: endpoint-breakdown
    endpointListType: errorRate
  - id: latency
    title: "Latency"
    type: latency
    layout:
      col: 8
    dataSource: metrics-timeseries
    endpointDataSource: endpoint-breakdown
    endpointListType: latency
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

const defaultTraces = `page: traces
title: "Traces"
icon: "GitBranch"
subtitle: "Distributed trace analysis — latency and error trends"

dataSources:
  - id: metrics-timeseries
    endpoint: /v1/metrics/timeseries
    params:
      interval: "5m"
  - id: endpoints-timeseries
    endpoint: /v1/endpoints/timeseries
  - id: endpoints-metrics
    endpoint: /v1/endpoints/metrics
  - id: latency-histogram
    endpoint: /v1/latency/histogram

charts:
  - id: trace-duration-distribution
    title: "Trace Duration Distribution"
    type: latency-histogram
    titleIcon: Clock
    layout:
      col: 12
    dataSource: latency-histogram
    height: 160
  - id: latency-distribution
    title: "Latency Distribution (Timeseries)"
    type: latency
    titleIcon: Clock
    layout:
      col: 12
    dataSource: metrics-timeseries
    endpointDataSource: endpoints-timeseries
    endpointMetricsSource: endpoints-metrics
    endpointListType: latency
    height: 240
  - id: error-rate
    title: "Error Rate"
    type: error-rate
    titleIcon: AlertCircle
    layout:
      col: 24
    dataSource: metrics-timeseries
    endpointDataSource: endpoints-timeseries
    endpointMetricsSource: endpoints-metrics
    endpointListType: errorRate
    height: 240
`

const defaultInfrastructure = `page: infrastructure
title: "Infrastructure"
icon: "Server"
subtitle: "Request volume and error rates grouped by host and pod"

dataSources:
  - id: service-timeseries
    endpoint: /v1/services/timeseries
    params:
      interval: "5m"
  - id: services-metrics
    endpoint: /v1/services/metrics

charts:
  - id: request-rate
    title: "Request Rate by Service"
    type: request
    titleIcon: Activity
    layout:
      col: 12
    dataSource: service-timeseries
    groupByKey: service
    valueKey: request_count
    datasetLabel: "Requests"
    height: 240
  - id: error-rate
    title: "Error Rate by Service"
    type: error-rate
    titleIcon: AlertCircle
    layout:
      col: 12
    dataSource: service-timeseries
    groupByKey: service
    endpointMetricsSource: services-metrics
    endpointListType: errorRate
    height: 240
`

const defaultServices = `page: services
title: "Services"
icon: "Layers"
subtitle: "Per-service request rate, error rate, and latency trends"

dataSources:
  - id: service-timeseries
    endpoint: /v1/services/timeseries
    params:
      interval: "5m"
  - id: services-metrics
    endpoint: /v1/services/metrics

charts:
  - id: request-rate
    title: "Request Rate by Service"
    type: request
    titleIcon: Activity
    layout:
      col: 12
    dataSource: service-timeseries
    groupByKey: service
    valueKey: request_count
    datasetLabel: "Requests"
    endpointMetricsSource: services-metrics
    endpointListType: requests
    height: 240
  - id: error-rate
    title: "Error Rate by Service"
    type: error-rate
    titleIcon: AlertCircle
    layout:
      col: 12
    dataSource: service-timeseries
    groupByKey: service
    endpointMetricsSource: services-metrics
    endpointListType: errorRate
    height: 240
  - id: latency
    title: "Avg Latency by Service"
    type: latency
    titleIcon: Clock
    layout:
      col: 24
    dataSource: service-timeseries
    groupByKey: service
    valueKey: avg_latency
    datasetLabel: "Avg Latency"
    height: 240
`

const defaultAiObservability = `page: ai-observability
title: "AI Observability"
icon: "Brain"
subtitle: "Performance, cost, and security visibility for LLM / AI model calls"

dataSources:
  - id: ai-perf-ts
    endpoint: /v1/ai/performance/timeseries
    params:
      interval: "5m"
  - id: ai-cost-ts
    endpoint: /v1/ai/cost/timeseries
    params:
      interval: "5m"
  - id: ai-sec-ts
    endpoint: /v1/ai/security/timeseries
    params:
      interval: "5m"
  - id: ai-lat-hist
    endpoint: /v1/ai/performance/latency-histogram
  - id: ai-cost-metrics
    endpoint: /v1/ai/cost/metrics
  - id: ai-token-breakdown
    endpoint: /v1/ai/cost/token-breakdown

charts:
  - id: qps
    title: "QPS per Model"
    type: ai-line
    titleIcon: TrendingDown
    layout:
      col: 12
    dataSource: ai-perf-ts
    groupByKey: model_name
    valueKey: qps
    height: 220
  - id: avg-latency
    title: "Avg Latency ms per Model"
    type: ai-line
    titleIcon: Clock
    layout:
      col: 12
    dataSource: ai-perf-ts
    groupByKey: model_name
    valueKey: avg_latency_ms
    height: 220
  - id: tokens-per-sec
    title: "Tokens / sec per Model"
    type: ai-line
    titleIcon: Zap
    layout:
      col: 12
    dataSource: ai-perf-ts
    groupByKey: model_name
    valueKey: tokens_per_sec
    height: 220
  - id: cost-over-time
    title: "Cost per Interval (USD)"
    type: ai-line
    titleIcon: Activity
    layout:
      col: 12
    dataSource: ai-cost-ts
    groupByKey: model_name
    valueKey: cost_per_interval
    yPrefix: "$"
    yDecimals: 4
    height: 220
  - id: pii-detections
    title: "PII Detections per Model"
    type: ai-line
    titleIcon: ShieldCheck
    layout:
      col: 12
    dataSource: ai-sec-ts
    groupByKey: model_name
    valueKey: pii_count
    height: 220
  - id: guardrail-blocks
    title: "Guardrail Blocks per Model"
    type: ai-line
    titleIcon: AlertCircle
    layout:
      col: 12
    dataSource: ai-sec-ts
    groupByKey: model_name
    valueKey: guardrail_count
    height: 220
  - id: latency-histogram
    title: "Latency Distribution (100ms buckets)"
    type: ai-bar
    titleIcon: BarChart3
    layout:
      col: 12
    dataSource: ai-lat-hist
    groupByKey: model_name
    bucketKey: bucket_ms
    valueKey: request_count
    height: 220
  - id: cost-by-model
    title: "Total Cost by Model (USD)"
    type: ai-bar
    titleIcon: Activity
    layout:
      col: 12
    dataSource: ai-cost-metrics
    labelKey: model_name
    valueKey: total_cost_usd
    color: "#F79009"
    yPrefix: "$"
    yDecimals: 4
    height: 220
  - id: token-breakdown
    title: "Token Breakdown by Model"
    type: ai-bar
    titleIcon: Zap
    layout:
      col: 24
    dataSource: ai-token-breakdown
    labelKey: model_name
    stacked: true
    valueKeys:
      - prompt_tokens
      - completion_tokens
      - system_tokens
      - cache_tokens
    height: 260
`

const defaultLatencyAnalysis = `page: latency-analysis
title: "Latency Analysis"
icon: "Clock"
subtitle: "Latency histogram and heatmap for trace duration analysis"

dataSources:
  - id: latency-histogram
    endpoint: /v1/latency/histogram
  - id: latency-heatmap
    endpoint: /v1/latency/heatmap
    params:
      interval: "5m"

charts:
  - id: histogram
    title: "Latency Histogram"
    type: latency-histogram
    titleIcon: BarChart3
    layout:
      col: 24
    dataSource: latency-histogram
    height: 200
  - id: heatmap
    title: "Latency Heatmap"
    type: latency-heatmap
    titleIcon: Activity
    layout:
      col: 24
    dataSource: latency-heatmap
`
