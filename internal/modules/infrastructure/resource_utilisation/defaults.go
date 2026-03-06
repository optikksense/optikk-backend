package resource_utilisation

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("resource-utilization", defaultResourceUtilization)
	dashboardconfig.RegisterDefaultConfig("jvm", defaultJVM)
}

const defaultResourceUtilization = `page: resource-utilization
title: "Resource Utilization"
icon: "Cpu"
subtitle: "CPU, memory, disk, network and connection pool utilization by service/instance"

dataSources:
  - id: resource-utilization
    endpoint: /v1/infrastructure/resource-utilisation/avg-cpu
    key: avgCpu
  - id: resource-utilization-memory
    endpoint: /v1/infrastructure/resource-utilisation/avg-memory
    key: avgMemory
  - id: resource-utilization-network
    endpoint: /v1/infrastructure/resource-utilisation/avg-network
    key: avgNetwork
  - id: resource-utilization-connpool
    endpoint: /v1/infrastructure/resource-utilisation/avg-conn-pool
    key: avgConnPool
  - id: cpu-usage-pct
    endpoint: /v1/infrastructure/resource-utilisation/cpu-usage-percentage
  - id: memory-usage-pct
    endpoint: /v1/infrastructure/resource-utilisation/memory-usage-percentage
  - id: by-service
    endpoint: /v1/infrastructure/resource-utilisation/by-service
  - id: by-instance
    endpoint: /v1/infrastructure/resource-utilisation/by-instance
  - id: infra-cpu-time
    endpoint: /v1/infrastructure/cpu-time
  - id: infra-memory-usage
    endpoint: /v1/infrastructure/memory-usage
  - id: infra-swap-usage
    endpoint: /v1/infrastructure/swap-usage
  - id: infra-disk-io
    endpoint: /v1/infrastructure/disk-io
  - id: infra-disk-operations
    endpoint: /v1/infrastructure/disk-operations
  - id: infra-disk-io-time
    endpoint: /v1/infrastructure/disk-io-time
  - id: infra-filesystem-usage
    endpoint: /v1/infrastructure/filesystem-usage
  - id: infra-filesystem-util
    endpoint: /v1/infrastructure/filesystem-utilization
  - id: infra-network-io
    endpoint: /v1/infrastructure/network-io
  - id: infra-network-packets
    endpoint: /v1/infrastructure/network-packets
  - id: infra-network-errors
    endpoint: /v1/infrastructure/network-errors
  - id: infra-network-dropped
    endpoint: /v1/infrastructure/network-dropped
  - id: infra-load-average
    endpoint: /v1/infrastructure/load-average
  - id: infra-process-count
    endpoint: /v1/infrastructure/process-count
  - id: infra-network-connections
    endpoint: /v1/infrastructure/network-connections

statCards:
  - title: "Avg CPU"
    dataSource: resource-utilization
    valueField: value
    formatter: percent1
    icon: Cpu
  - title: "Avg Memory"
    dataSource: resource-utilization-memory
    valueField: value
    formatter: percent1
    icon: HardDrive
  - title: "Avg Network"
    dataSource: resource-utilization-network
    valueField: value
    formatter: percent1
    icon: Network
  - title: "Avg Conn Pool"
    dataSource: resource-utilization-connpool
    valueField: value
    formatter: percent1
    icon: Database

charts:
  - id: cpu-usage
    title: "CPU Usage Percentage"
    type: request
    titleIcon: Cpu
    layout:
      col: 12
    dataSource: cpu-usage-pct
    groupByKey: pod
    valueKey: value
    datasetLabel: "CPU Util"
    height: 260
  - id: memory-usage
    title: "Memory Usage Percentage"
    type: request
    titleIcon: HardDrive
    layout:
      col: 12
    dataSource: memory-usage-pct
    groupByKey: pod
    valueKey: value
    datasetLabel: "Mem Util"
    height: 260
  - id: by-service
    title: "Resource Usage by Service"
    type: table
    titleIcon: Layers
    layout:
      col: 24
    dataSource: by-service
    height: 320
  - id: by-instance
    title: "Resource Usage by Instance"
    type: table
    titleIcon: Server
    layout:
      col: 24
    dataSource: by-instance
    height: 320
  - id: infra-cpu-time
    title: "CPU Time by State"
    type: area
    titleIcon: Cpu
    layout:
      col: 12
    dataSource: infra-cpu-time
    groupByKey: state
    valueKey: value
    height: 260
  - id: infra-memory-usage
    title: "Memory Usage by State"
    type: area
    titleIcon: HardDrive
    layout:
      col: 12
    dataSource: infra-memory-usage
    groupByKey: state
    valueKey: value
    height: 260
  - id: infra-swap-usage
    title: "Swap Usage by State"
    type: area
    titleIcon: HardDrive
    layout:
      col: 12
    dataSource: infra-swap-usage
    groupByKey: state
    valueKey: value
    height: 260
  - id: infra-load-average
    title: "Load Average (1m / 5m / 15m)"
    type: stat
    titleIcon: Activity
    layout:
      col: 12
    dataSource: infra-load-average
    height: 120
  - id: infra-disk-io
    title: "Disk I/O (Read / Write)"
    type: area
    titleIcon: HardDrive
    layout:
      col: 12
    dataSource: infra-disk-io
    groupByKey: direction
    valueKey: value
    height: 260
  - id: infra-disk-operations
    title: "Disk Operations (Read / Write)"
    type: area
    titleIcon: HardDrive
    layout:
      col: 12
    dataSource: infra-disk-operations
    groupByKey: direction
    valueKey: value
    height: 260
  - id: infra-disk-io-time
    title: "Disk I/O Time"
    type: area
    titleIcon: Clock
    layout:
      col: 12
    dataSource: infra-disk-io-time
    groupByKey: pod
    valueKey: value
    height: 260
  - id: infra-filesystem-usage
    title: "Filesystem Usage by Mountpoint"
    type: area
    titleIcon: FolderOpen
    layout:
      col: 12
    dataSource: infra-filesystem-usage
    groupByKey: mountpoint
    valueKey: value
    height: 260
  - id: infra-filesystem-util
    title: "Filesystem Utilization"
    type: area
    titleIcon: FolderOpen
    layout:
      col: 12
    dataSource: infra-filesystem-util
    groupByKey: pod
    valueKey: value
    height: 260
  - id: infra-network-io
    title: "Network I/O (Transmit / Receive)"
    type: area
    titleIcon: Network
    layout:
      col: 12
    dataSource: infra-network-io
    groupByKey: direction
    valueKey: value
    height: 260
  - id: infra-network-packets
    title: "Network Packets (Transmit / Receive)"
    type: area
    titleIcon: Network
    layout:
      col: 12
    dataSource: infra-network-packets
    groupByKey: direction
    valueKey: value
    height: 260
  - id: infra-network-errors
    title: "Network Errors by State"
    type: area
    titleIcon: AlertTriangle
    layout:
      col: 12
    dataSource: infra-network-errors
    groupByKey: state
    valueKey: value
    height: 260
  - id: infra-network-dropped
    title: "Dropped Packets"
    type: area
    titleIcon: AlertTriangle
    layout:
      col: 12
    dataSource: infra-network-dropped
    groupByKey: pod
    valueKey: value
    height: 260
  - id: infra-process-count
    title: "Process Count by Status"
    type: area
    titleIcon: Activity
    layout:
      col: 12
    dataSource: infra-process-count
    groupByKey: state
    valueKey: value
    height: 260
  - id: infra-network-connections
    title: "Network Connections by State"
    type: area
    titleIcon: Network
    layout:
      col: 12
    dataSource: infra-network-connections
    groupByKey: state
    valueKey: value
    height: 260
`

const defaultJVM = `page: jvm
title: "JVM Runtime"
icon: "Coffee"
subtitle: "JVM memory, garbage collection, threads, classes, CPU, and buffer pool metrics"

dataSources:
  - id: jvm-memory
    endpoint: /v1/infrastructure/jvm/memory
  - id: jvm-gc-duration
    endpoint: /v1/infrastructure/jvm/gc-duration
  - id: jvm-gc-collections
    endpoint: /v1/infrastructure/jvm/gc-collections
  - id: jvm-threads
    endpoint: /v1/infrastructure/jvm/threads
  - id: jvm-classes
    endpoint: /v1/infrastructure/jvm/classes
  - id: jvm-cpu
    endpoint: /v1/infrastructure/jvm/cpu
  - id: jvm-buffers
    endpoint: /v1/infrastructure/jvm/buffers

statCards:
  - title: "GC p95 Duration"
    dataSource: jvm-gc-duration
    valueField: p95
    formatter: ms
    icon: Trash2
  - title: "GC p99 Duration"
    dataSource: jvm-gc-duration
    valueField: p99
    formatter: ms
    icon: Trash2
  - title: "JVM CPU Time"
    dataSource: jvm-cpu
    valueField: cpu_time_value
    formatter: ns
    icon: Cpu
  - title: "JVM CPU Utilization"
    dataSource: jvm-cpu
    valueField: recent_utilization
    formatter: percent2
    icon: Cpu

charts:
  - id: jvm-memory
    title: "JVM Memory (Used / Committed / Limit)"
    type: area
    titleIcon: HardDrive
    layout:
      col: 24
    dataSource: jvm-memory
    groupByKey: pool_name
    valueKey: used
    height: 300
  - id: jvm-gc-collections
    title: "GC Collections by Collector"
    type: area
    titleIcon: Trash2
    layout:
      col: 12
    dataSource: jvm-gc-collections
    groupByKey: pod
    valueKey: value
    height: 260
  - id: jvm-threads
    title: "Thread Count by State"
    type: area
    titleIcon: Activity
    layout:
      col: 12
    dataSource: jvm-threads
    groupByKey: state
    valueKey: value
    height: 260
  - id: jvm-buffers
    title: "Buffer Pool Memory Usage"
    type: area
    titleIcon: Database
    layout:
      col: 12
    dataSource: jvm-buffers
    groupByKey: pool_name
    valueKey: memory_usage
    height: 260
  - id: jvm-classes
    title: "Loaded Classes"
    type: stat
    titleIcon: BookOpen
    layout:
      col: 12
    dataSource: jvm-classes
    height: 120
`
