package kubernetes

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("kubernetes", defaultKubernetes)
}

const defaultKubernetes = `page: kubernetes
title: "Kubernetes"
icon: "Layers"
subtitle: "Container CPU, memory, OOM kills, pod health, replica status, and volumes"

dataSources:
  - id: k8s-container-cpu
    endpoint: /v1/infrastructure/kubernetes/container-cpu
  - id: k8s-cpu-throttling
    endpoint: /v1/infrastructure/kubernetes/cpu-throttling
  - id: k8s-container-memory
    endpoint: /v1/infrastructure/kubernetes/container-memory
  - id: k8s-oom-kills
    endpoint: /v1/infrastructure/kubernetes/oom-kills
  - id: k8s-pod-restarts
    endpoint: /v1/infrastructure/kubernetes/pod-restarts
  - id: k8s-node-allocatable
    endpoint: /v1/infrastructure/kubernetes/node-allocatable
  - id: k8s-pod-phases
    endpoint: /v1/infrastructure/kubernetes/pod-phases
  - id: k8s-replica-status
    endpoint: /v1/infrastructure/kubernetes/replica-status
  - id: k8s-volume-usage
    endpoint: /v1/infrastructure/kubernetes/volume-usage

statCards:
  - title: "Allocatable CPU (cores)"
    dataSource: k8s-node-allocatable
    valueField: cpu_cores
    formatter: number
    icon: Cpu
  - title: "Allocatable Memory"
    dataSource: k8s-node-allocatable
    valueField: memory_bytes
    formatter: bytes
    icon: HardDrive

charts:
  - id: k8s-container-cpu
    title: "Container CPU Time"
    type: area
    titleIcon: Cpu
    layout:
      col: 12
    dataSource: k8s-container-cpu
    groupByKey: container
    valueKey: value
    height: 260
  - id: k8s-cpu-throttling
    title: "CPU Throttling Time"
    type: area
    titleIcon: AlertTriangle
    layout:
      col: 12
    dataSource: k8s-cpu-throttling
    groupByKey: container
    valueKey: value
    height: 260
  - id: k8s-container-memory
    title: "Container Memory Usage"
    type: area
    titleIcon: HardDrive
    layout:
      col: 12
    dataSource: k8s-container-memory
    groupByKey: container
    valueKey: value
    height: 260
  - id: k8s-oom-kills
    title: "OOM Kill Count"
    type: area
    titleIcon: AlertTriangle
    layout:
      col: 12
    dataSource: k8s-oom-kills
    groupByKey: container
    valueKey: value
    height: 260
  - id: k8s-pod-phases
    title: "Pod Phase Distribution"
    type: pie
    titleIcon: Circle
    layout:
      col: 12
    dataSource: k8s-pod-phases
    groupByKey: phase
    valueKey: count
    height: 260
  - id: k8s-replica-status
    title: "ReplicaSet Status (Desired vs Available)"
    type: table
    titleIcon: Layers
    layout:
      col: 12
    dataSource: k8s-replica-status
    height: 260
  - id: k8s-pod-restarts
    title: "Pod Restarts"
    type: table
    titleIcon: RefreshCw
    layout:
      col: 12
    dataSource: k8s-pod-restarts
    height: 320
  - id: k8s-volume-usage
    title: "Volume Usage"
    type: table
    titleIcon: FolderOpen
    layout:
      col: 12
    dataSource: k8s-volume-usage
    height: 320
`
