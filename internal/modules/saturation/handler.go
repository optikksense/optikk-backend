package saturation

import (
	"net/http"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/saturation/service"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"
)

// SaturationHandler handles saturation page endpoints.
type SaturationHandler struct {
	modulecommon.DBTenant
	Service service.Service
}

// GetSaturationMetrics returns resource saturation summary per service.
func (h *SaturationHandler) GetSaturationMetrics(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Service.GetSaturationMetrics(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query saturation metrics")
		return
	}

	RespondOK(c, rows)
}

// GetSaturationTimeSeries returns saturation metrics over time per service.
func (h *SaturationHandler) GetSaturationTimeSeries(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Service.GetSaturationTimeSeries(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query saturation timeseries")
		return
	}

	RespondOK(c, rows)
}
