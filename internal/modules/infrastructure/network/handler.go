package network

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type NetworkHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *NetworkHandler) GetNetworkIO(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkIO(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query network I/O", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *NetworkHandler) GetNetworkPackets(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkPackets(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query network packets", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *NetworkHandler) GetNetworkErrors(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkErrors(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query network errors", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *NetworkHandler) GetNetworkDropped(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkDropped(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query dropped packets", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *NetworkHandler) GetNetworkConnections(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkConnections(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query network connections", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *NetworkHandler) GetAvgNetwork(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetAvgNetwork(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg network", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *NetworkHandler) GetNetworkByService(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "serviceName is required")
		return
	}
	resp, err := h.Service.GetNetworkByService(c.Request.Context(), teamID, serviceName, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query network by service", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *NetworkHandler) GetNetworkByInstance(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	host := c.Query("host")
	pod := c.Query("pod")
	container := c.Query("container")
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "serviceName is required")
		return
	}
	resp, err := h.Service.GetNetworkByInstance(c.Request.Context(), teamID, host, pod, container, serviceName, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query network by instance", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
