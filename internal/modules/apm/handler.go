package apm

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// APMHandler handles API endpoints for APM metrics.
type APMHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *APMHandler) GetRPCDuration(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetRPCDuration(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query RPC duration")
		return
	}
	RespondOK(c, resp)
}

func (h *APMHandler) GetRPCRequestRate(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetRPCRequestRate(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query RPC request rate")
		return
	}
	RespondOK(c, resp)
}

func (h *APMHandler) GetMessagingPublishDuration(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetMessagingPublishDuration(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query messaging publish duration")
		return
	}
	RespondOK(c, resp)
}

func (h *APMHandler) GetProcessCPU(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetProcessCPU(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query process CPU")
		return
	}
	RespondOK(c, resp)
}

func (h *APMHandler) GetProcessMemory(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetProcessMemory(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query process memory")
		return
	}
	RespondOK(c, resp)
}

func (h *APMHandler) GetOpenFDs(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetOpenFDs(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query open file descriptors")
		return
	}
	RespondOK(c, resp)
}

func (h *APMHandler) GetUptime(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetUptime(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query process uptime")
		return
	}
	RespondOK(c, resp)
}
