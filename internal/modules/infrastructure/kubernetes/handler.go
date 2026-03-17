package kubernetes

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type KubernetesHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *KubernetesHandler) GetContainerCPU(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetContainerCPU(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query container CPU")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetCPUThrottling(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetCPUThrottling(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query CPU throttling")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetContainerMemory(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetContainerMemory(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query container memory")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetOOMKills(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetOOMKills(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query OOM kills")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetPodRestarts(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetPodRestarts(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query pod restarts")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetNodeAllocatable(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetNodeAllocatable(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query node allocatable resources")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetPodPhases(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetPodPhases(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query pod phases")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetReplicaStatus(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetReplicaStatus(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query replica status")
		return
	}
	RespondOK(c, resp)
}

func (h *KubernetesHandler) GetVolumeUsage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetVolumeUsage(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query volume usage")
		return
	}
	RespondOK(c, resp)
}
