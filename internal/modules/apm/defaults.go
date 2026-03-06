package apm

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("apm", defaultAPM)
}

const defaultAPM = `page: apm
title: "APM"
icon: "Zap"
subtitle: "RPC performance, messaging publish latency, and process-level resource usage"

dataSources:
  - id: apm-rpc-duration
    endpoint: /v1/apm/rpc-duration
  - id: apm-rpc-request-rate
    endpoint: /v1/apm/rpc-request-rate
  - id: apm-messaging-duration
    endpoint: /v1/apm/messaging-publish-duration
  - id: apm-process-cpu
    endpoint: /v1/apm/process-cpu
  - id: apm-process-memory
    endpoint: /v1/apm/process-memory
  - id: apm-open-fds
    endpoint: /v1/apm/open-fds
  - id: apm-uptime
    endpoint: /v1/apm/uptime

statCards:
  - title: "RPC p95 Duration"
    dataSource: apm-rpc-duration
    valueField: p95
    formatter: ms
    icon: Clock
  - title: "RPC p99 Duration"
    dataSource: apm-rpc-duration
    valueField: p99
    formatter: ms
    icon: Clock
  - title: "RSS Memory"
    dataSource: apm-process-memory
    valueField: rss
    formatter: bytes
    icon: HardDrive
  - title: "Virtual Memory"
    dataSource: apm-process-memory
    valueField: vms
    formatter: bytes
    icon: HardDrive

charts:
  - id: apm-rpc-request-rate
    title: "RPC Request Rate"
    type: area
    titleIcon: Activity
    layout:
      col: 12
    dataSource: apm-rpc-request-rate
    valueKey: value
    height: 260
  - id: apm-messaging-duration
    title: "Messaging Publish Duration p50/p95/p99"
    type: stat
    titleIcon: Send
    layout:
      col: 12
    dataSource: apm-messaging-duration
    height: 120
  - id: apm-process-cpu
    title: "Process CPU Time by State"
    type: area
    titleIcon: Cpu
    layout:
      col: 12
    dataSource: apm-process-cpu
    groupByKey: state
    valueKey: value
    height: 260
  - id: apm-open-fds
    title: "Open File Descriptors"
    type: area
    titleIcon: FileText
    layout:
      col: 12
    dataSource: apm-open-fds
    valueKey: value
    height: 260
  - id: apm-uptime
    title: "Process Uptime"
    type: area
    titleIcon: Clock
    layout:
      col: 24
    dataSource: apm-uptime
    valueKey: value
    height: 260
`
