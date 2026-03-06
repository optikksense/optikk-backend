package kubernetes

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// KubernetesHandler handles API endpoints for Kubernetes metrics.
type KubernetesHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *KubernetesHandler) GetContainerCPU(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetContainerCPU(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query container CPU")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetCPUThrottling(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetCPUThrottling(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query CPU throttling")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetContainerMemory(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetContainerMemory(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query container memory")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetOOMKills(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetOOMKills(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query OOM kills")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetPodRestarts(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetPodRestarts(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query pod restarts")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetNodeAllocatable(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetNodeAllocatable(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query node allocatable resources")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetPodPhases(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetPodPhases(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query pod phases")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetReplicaStatus(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetReplicaStatus(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query replica status")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetVolumeUsage(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)
	resp, err := h.Service.GetVolumeUsage(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query volume usage")
		return
	}
	RespondOK(c, resp)
}
