package resource_utilisation //nolint:misspell

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/platform/logger"
	"go.uber.org/zap"
)

type ResourceUtilisationHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *ResourceUtilisationHandler) GetAvgCPU(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetAvgCPU(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg CPU", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetAvgMemory(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetAvgMemory(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg Memory", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetAvgNetwork(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetAvgNetwork(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg Network", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetAvgConnPool(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetAvgConnPool(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg Conn Pool", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetCPUUsagePercentage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetCPUUsagePercentage(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query CPU usage percentage", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetMemoryUsagePercentage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetMemoryUsagePercentage(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query Memory usage percentage", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetByService(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetResourceUsageByService(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query resource usage by service", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetByInstance(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetResourceUsageByInstance(teamID, startMs, endMs)
	if err != nil {
		logger.L().Error("resource usage by instance query failed", zap.Error(err))
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query resource usage by instance", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
