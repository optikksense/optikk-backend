package servicemap

import "github.com/observability/observability-backend-go/internal/modules/dashboardconfig"

func init() {
	dashboardconfig.RegisterDefaultConfig("service-map", defaultServiceMap)
}

const defaultServiceMap = `page: service-map
title: "Service Map"
icon: "Share2"
subtitle: "Service dependency graph, external integrations, and latency gaps"

tabs:
  - id: graph
    label: Graph
    dataSources:
      - id: topology
        endpoint: /v1/services/topology
      - id: external-deps
        endpoint: /v1/services/external-dependencies
    charts:
      - id: service-topology
        title: "Service Dependency Graph"
        type: service-map
        titleIcon: Share2
        layout:
          col: 24
        dataSource: topology
        height: 560
      - id: external-dependencies
        title: "External Dependencies"
        type: table
        titleIcon: Globe
        layout:
          col: 24
        dataSource: external-deps
        height: 320

  - id: dependencies
    label: Dependencies
    dataSources:
      - id: upstream-downstream
        endpoint: /v1/services/{serviceName}/upstream-downstream
    charts:
      - id: upstream-downstream
        title: "Upstream / Downstream Table"
        type: table
        titleIcon: ArrowUpDown
        layout:
          col: 24
        dataSource: upstream-downstream
        height: 480

  - id: latency
    label: Latency Gap
    dataSources:
      - id: client-server-latency
        endpoint: /v1/spans/client-server-latency
    charts:
      - id: client-server-latency
        title: "Client vs Server Latency (p95)"
        type: latency
        titleIcon: Activity
        layout:
          col: 24
        dataSource: client-server-latency
        height: 320
`
