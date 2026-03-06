package errortracking

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// ErrorTrackingHandler handles error tracking endpoints.
type ErrorTrackingHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetExceptionRateByType returns time-series exception counts grouped by exception.type.
func (h *ErrorTrackingHandler) GetExceptionRateByType(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	serviceName := c.Query("serviceName")

	points, err := h.Service.GetExceptionRateByType(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query exception rate by type")
		return
	}
	RespondOK(c, points)
}

// GetErrorHotspot returns error_rate per (service × operation) for a heatmap.
func (h *ErrorTrackingHandler) GetErrorHotspot(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	cells, err := h.Service.GetErrorHotspot(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error hotspot")
		return
	}
	RespondOK(c, cells)
}

// GetHTTP5xxByRoute returns counts of HTTP 5xx responses per route.
func (h *ErrorTrackingHandler) GetHTTP5xxByRoute(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	serviceName := c.Query("serviceName")

	rows, err := h.Service.GetHTTP5xxByRoute(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query HTTP 5xx by route")
		return
	}
	RespondOK(c, rows)
}
