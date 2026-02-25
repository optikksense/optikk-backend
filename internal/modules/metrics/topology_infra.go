package metrics

import (
	. "github.com/observability/observability-backend-go/internal/platform/handlers"

	"github.com/gin-gonic/gin"
)

// GetServiceTopology — service graph nodes + edges.
func (h *MetricHandler) GetServiceTopology(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	serviceMetrics, _ := h.Repo.GetServiceTopologyNodes(teamUUID, startMs, endMs)
	deps, _ := h.Repo.GetServiceTopologyEdges(teamUUID, startMs, endMs)

	nodes := make([]map[string]any, 0, len(serviceMetrics))
	for _, s := range serviceMetrics {
		requestCount := Int64FromAny(s["request_count"])
		errorCount := Int64FromAny(s["error_count"])
		errorRate := calcErrorRate(requestCount, errorCount)
		nodes = append(nodes, map[string]any{
			"name":         s["service_name"],
			"status":       topologyNodeStatus(errorRate),
			"requestCount": requestCount,
			"errorRate":    errorRate,
			"avgLatency":   s["avg_latency"],
		})
	}

	edges := make([]map[string]any, 0, len(deps))
	for _, d := range deps {
		edges = append(edges, map[string]any{
			"source":     d["source"],
			"target":     d["target"],
			"callCount":  d["call_count"],
			"avgLatency": d["avg_latency"],
			"errorRate":  d["error_rate"],
		})
	}

	RespondOK(c, map[string]any{"nodes": nodes, "edges": edges})
}
