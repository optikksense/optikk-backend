package resource_utilisation

import (
	"net/http"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/resource_utilisation/service"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"
)

// ResourceUtilisationHandler handles API endpoints for 8 individual metrics.
type ResourceUtilisationHandler struct {
	modulecommon.DBTenant
	Service service.Service
}

func (h *ResourceUtilisationHandler) GetAvgCPU(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetAvgCPU(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query avg CPU")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetAvgMemory(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetAvgMemory(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query avg Memory")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetAvgNetwork(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetAvgNetwork(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query avg Network")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetAvgConnPool(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetAvgConnPool(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query avg Conn Pool")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetCPUUsagePercentage(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetCPUUsagePercentage(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query CPU usage percentage")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetMemoryUsagePercentage(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetMemoryUsagePercentage(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Memory usage percentage")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetByService(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetResourceUsageByService(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query resource usage by service")
		return
	}
	RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetByInstance(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	resp, err := h.Service.GetResourceUsageByInstance(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query resource usage by instance")
		return
	}
	RespondOK(c, resp)
}
