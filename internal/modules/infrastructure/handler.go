package infrastructure

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"
)

// InfrastructureHandler handles infrastructure page endpoints.
type InfrastructureHandler struct {
	modulecommon.DBTenant
	Repo *Repository
}

// GetInfrastructure returns host/pod/container level resource summary.
func (h *InfrastructureHandler) GetInfrastructure(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Repo.GetInfrastructure(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query infrastructure metrics")
		return
	}

	for _, row := range rows {
		row["services"] = splitCSV(StringFromAny(row["services_csv"]))
		row["spanCount"] = Int64FromAny(row["span_count"])
		row["errorCount"] = Int64FromAny(row["error_count"])
		row["avgLatency"] = Float64FromAny(row["avg_latency"])
		row["p95Latency"] = Float64FromAny(row["p95_latency"])
		delete(row, "services_csv")
	}

	RespondOK(c, NormalizeRows(rows))
}

// GetInfrastructureNodes returns host-level aggregation for the nodes view.
func (h *InfrastructureHandler) GetInfrastructureNodes(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Repo.GetInfrastructureNodes(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query node health")
		return
	}

	for _, row := range rows {
		row["services"] = splitCSV(StringFromAny(row["services_csv"]))
		row["avg_latency_ms"] = Float64FromAny(row["avg_latency"])
		row["p95_latency_ms"] = Float64FromAny(row["p95_latency"])
		delete(row, "services_csv")
	}

	RespondOK(c, NormalizeRows(rows))
}

// GetInfrastructureNodeServices returns services running on a specific host.
func (h *InfrastructureHandler) GetInfrastructureNodeServices(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	host := c.Param("host")
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Repo.GetInfrastructureNodeServices(teamUUID, host, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query node services")
		return
	}

	for _, row := range rows {
		row["avg_latency_ms"] = Float64FromAny(row["avg_latency"])
		row["p95_latency_ms"] = Float64FromAny(row["p95_latency"])
	}

	RespondOK(c, NormalizeRows(rows))
}

func splitCSV(s string) []string {
	// ClickHouse groupArray returns values like ['item1','item2']
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	parts := strings.Split(s, ",")
	clean := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "'\"")
		if p != "" {
			clean = append(clean, p)
		}
	}
	return clean
}
