package resource_utilisation

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
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
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetAvgCPU(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg CPU", err)
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetAvgMemory(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetAvgMemory(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg Memory", err)
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetAvgNetwork(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetAvgNetwork(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg Network", err)
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetAvgConnPool(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetAvgConnPool(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg Conn Pool", err)
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetCPUUsagePercentage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetCPUUsagePercentage(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query CPU usage percentage", err)
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetMemoryUsagePercentage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetMemoryUsagePercentage(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query Memory usage percentage", err)
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetByService(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetResourceUsageByService(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query resource usage by service", err)
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetByInstance(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetResourceUsageByInstance(teamID, startMs, endMs)
	if err != nil {
		logger.L().Error("resource usage by instance query failed", zap.Error(err))
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query resource usage by instance", err)
		return
	}
	RespondOK(c, resp)
}
