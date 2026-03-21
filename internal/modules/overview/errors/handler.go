package errors

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

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
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetServiceErrorRate(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query service error rate", err)
		return
	}

	RespondOK(c, points)
}

func (h *ErrorHandler) GetErrorVolume(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetErrorVolume(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error volume", err)
		return
	}

	RespondOK(c, points)
}

func (h *ErrorHandler) GetLatencyDuringErrorWindows(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetLatencyDuringErrorWindows(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency during error windows", err)
		return
	}

	RespondOK(c, points)
}

func (h *ErrorHandler) GetErrorGroups(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	limit := ParseIntParam(c, "limit", 100)
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	groups, err := h.Service.GetErrorGroups(c.Request.Context(), teamID, startMs, endMs, serviceName, limit)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query overview errors", err)
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
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error group detail", err)
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
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error group traces", err)
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
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error group timeseries", err)
		return
	}
	RespondOK(c, points)
}
