package errors

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// ErrorHandler handles overview error page endpoints.
type ErrorHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetServiceErrorRate returns service-level error-rate buckets for the errors dashboard.
func (h *ErrorHandler) GetServiceErrorRate(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetServiceErrorRate(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query service error rate")
		return
	}

	RespondOK(c, points)
}

// GetErrorVolume returns service-level error-volume buckets for the errors dashboard.
func (h *ErrorHandler) GetErrorVolume(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetErrorVolume(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error volume")
		return
	}

	RespondOK(c, points)
}

// GetLatencyDuringErrorWindows returns service latency buckets for windows that saw errors.
func (h *ErrorHandler) GetLatencyDuringErrorWindows(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetLatencyDuringErrorWindows(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query latency during error windows")
		return
	}

	RespondOK(c, points)
}

// GetErrorGroups returns grouped errors for the errors dashboard.
func (h *ErrorHandler) GetErrorGroups(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	limit := ParseIntParam(c, "limit", 100)
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	groups, err := h.Service.GetErrorGroups(teamUUID, startMs, endMs, serviceName, limit)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview errors")
		return
	}

	RespondOK(c, groups)
}
