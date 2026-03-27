package errortracking

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"

	"github.com/gin-gonic/gin"
)

type ErrorTrackingHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *ErrorTrackingHandler) GetExceptionRateByType(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}

	points, err := h.Service.GetExceptionRateByType(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query exception rate by type", err)
		return
	}
	modulecommon.RespondOK(c, points)
}

func (h *ErrorTrackingHandler) GetErrorHotspot(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	cells, err := h.Service.GetErrorHotspot(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error hotspot", err)
		return
	}
	modulecommon.RespondOK(c, cells)
}

func (h *ErrorTrackingHandler) GetHTTP5xxByRoute(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}

	rows, err := h.Service.GetHTTP5xxByRoute(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query HTTP 5xx by route", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}
