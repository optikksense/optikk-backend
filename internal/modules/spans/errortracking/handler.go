package errortracking

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type ErrorTrackingHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *ErrorTrackingHandler) GetExceptionRateByType(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")

	points, err := h.Service.GetExceptionRateByType(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query exception rate by type")
		return
	}
	RespondOK(c, points)
}

func (h *ErrorTrackingHandler) GetErrorHotspot(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	cells, err := h.Service.GetErrorHotspot(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error hotspot")
		return
	}
	RespondOK(c, cells)
}

func (h *ErrorTrackingHandler) GetHTTP5xxByRoute(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")

	rows, err := h.Service.GetHTTP5xxByRoute(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query HTTP 5xx by route")
		return
	}
	RespondOK(c, rows)
}
