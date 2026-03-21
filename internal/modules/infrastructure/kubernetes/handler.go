package kubernetes

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

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
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query container CPU", err)
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
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query CPU throttling", err)
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
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query container memory", err)
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
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query OOM kills", err)
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
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query pod restarts", err)
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
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query node allocatable resources", err)
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
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query pod phases", err)
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
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query replica status", err)
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
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query volume usage", err)
		return
	}
	RespondOK(c, resp)
}
