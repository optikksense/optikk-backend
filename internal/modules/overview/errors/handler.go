package errors

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type ErrorHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *ErrorHandler) GetServiceErrorRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetServiceErrorRate(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query service error rate", err)
		return
	}

	modulecommon.RespondOK(c, points)
}

func (h *ErrorHandler) GetErrorVolume(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetErrorVolume(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error volume", err)
		return
	}

	modulecommon.RespondOK(c, points)
}

func (h *ErrorHandler) GetLatencyDuringErrorWindows(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetLatencyDuringErrorWindows(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency during error windows", err)
		return
	}

	modulecommon.RespondOK(c, points)
}

func (h *ErrorHandler) GetErrorGroups(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	limit := modulecommon.ParseIntParam(c, "limit", 100)
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	groups, err := h.Service.GetErrorGroups(c.Request.Context(), teamID, startMs, endMs, serviceName, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview errors", err)
		return
	}

	modulecommon.RespondOK(c, groups)
}

func (h *ErrorHandler) GetErrorGroupDetail(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	groupID := c.Param("groupId")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	detail, err := h.Service.GetErrorGroupDetail(c.Request.Context(), teamID, startMs, endMs, groupID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error group detail", err)
		return
	}
	modulecommon.RespondOK(c, detail)
}

func (h *ErrorHandler) GetErrorGroupTraces(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	groupID := c.Param("groupId")
	limit := modulecommon.ParseIntParam(c, "limit", 50)
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	traces, err := h.Service.GetErrorGroupTraces(c.Request.Context(), teamID, startMs, endMs, groupID, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error group traces", err)
		return
	}
	modulecommon.RespondOK(c, traces)
}

func (h *ErrorHandler) GetErrorGroupTimeseries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	groupID := c.Param("groupId")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetErrorGroupTimeseries(c.Request.Context(), teamID, startMs, endMs, groupID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error group timeseries", err)
		return
	}
	modulecommon.RespondOK(c, points)
}
