package kubernetes

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/contracts/errorcode"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type KubernetesHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *KubernetesHandler) GetContainerCPU(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetContainerCPU(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query container CPU", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KubernetesHandler) GetCPUThrottling(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetCPUThrottling(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query CPU throttling", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KubernetesHandler) GetContainerMemory(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetContainerMemory(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query container memory", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KubernetesHandler) GetOOMKills(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetOOMKills(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query OOM kills", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KubernetesHandler) GetPodRestarts(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetPodRestarts(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query pod restarts", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KubernetesHandler) GetNodeAllocatable(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetNodeAllocatable(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query node allocatable resources", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KubernetesHandler) GetPodPhases(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetPodPhases(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query pod phases", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KubernetesHandler) GetReplicaStatus(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetReplicaStatus(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query replica status", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *KubernetesHandler) GetVolumeUsage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	node := c.Query("node")
	resp, err := h.Service.GetVolumeUsage(c.Request.Context(), teamID, startMs, endMs, node)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query volume usage", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
