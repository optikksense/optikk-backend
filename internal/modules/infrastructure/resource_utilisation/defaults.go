package resource_utilisation

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("resource-utilization", defaultResourceUtilization)
}

const defaultResourceUtilization = `page: resource-utilization
title: "Resource Utilization"
icon: "Cpu"
subtitle: "CPU, memory, disk, network and connection pool utilization by service/instance"

dataSources:
  - id: resource-utilization

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
