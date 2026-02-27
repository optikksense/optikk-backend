package insights

import (
	"fmt"
	"net/http"

	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/insights/service"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"

	"github.com/gin-gonic/gin"
)

// InsightHandler handles business-insight API endpoints.
type InsightHandler struct {
	modulecommon.DBTenant
	Service service.Service
}

// GetInsightResourceUtilization — CPU / memory / disk / network utilisation by service and instance.
func (h *InsightHandler) GetInsightResourceUtilization(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetInsightResourceUtilization(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query resource utilization")
		return
	}

	RespondOK(c, resp)
}

// GetInsightSloSli — SLO / SLI compliance status and trend.
func (h *InsightHandler) GetInsightSloSli(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs := ParseRange(c, 7*24*60*60*1000)

	resp, err := h.Service.GetInsightSloSli(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		fmt.Println("[SLO ERROR]", err)
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query SLO status")
		return
	}

	RespondOK(c, resp)
}

// GetInsightLogsStream — recent log stream with volume trend and correlation stats.
func (h *InsightHandler) GetInsightLogsStream(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	limit := ParseIntParam(c, "limit", 200)

	resp, err := h.Service.GetInsightLogsStream(teamUUID, startMs, endMs, limit)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query logs stream")
		return
	}

	RespondOK(c, resp)
}
