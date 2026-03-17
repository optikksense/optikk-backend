package errors

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type ErrorHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *ErrorHandler) GetServiceErrorRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetServiceErrorRate(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query service error rate")
		return
	}

	RespondOK(c, points)
}

func (h *ErrorHandler) GetErrorVolume(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetErrorVolume(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error volume")
		return
	}

	RespondOK(c, points)
}

func (h *ErrorHandler) GetLatencyDuringErrorWindows(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetLatencyDuringErrorWindows(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query latency during error windows")
		return
	}

	RespondOK(c, points)
}

func (h *ErrorHandler) GetErrorGroups(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	limit := ParseIntParam(c, "limit", 100)
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	groups, err := h.Service.GetErrorGroups(c.Request.Context(), teamID, startMs, endMs, serviceName, limit)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview errors")
		return
	}

	RespondOK(c, groups)
}

func (h *ErrorHandler) GetErrorGroupDetail(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	groupID := c.Param("groupId")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	detail, err := h.Service.GetErrorGroupDetail(c.Request.Context(), teamID, startMs, endMs, groupID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error group detail")
		return
	}
	RespondOK(c, detail)
}

func (h *ErrorHandler) GetErrorGroupTraces(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	groupID := c.Param("groupId")
	limit := ParseIntParam(c, "limit", 50)
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	traces, err := h.Service.GetErrorGroupTraces(c.Request.Context(), teamID, startMs, endMs, groupID, limit)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error group traces")
		return
	}
	RespondOK(c, traces)
}

func (h *ErrorHandler) GetErrorGroupTimeseries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	groupID := c.Param("groupId")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetErrorGroupTimeseries(c.Request.Context(), teamID, startMs, endMs, groupID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error group timeseries")
		return
	}
	RespondOK(c, points)
}
