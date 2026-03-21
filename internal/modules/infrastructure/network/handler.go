package network

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type NetworkHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *NetworkHandler) GetNetworkIO(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkIO(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query network I/O", err)
		return
	}
	RespondOK(c, resp)
}

func (h *NetworkHandler) GetNetworkPackets(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkPackets(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query network packets", err)
		return
	}
	RespondOK(c, resp)
}

func (h *NetworkHandler) GetNetworkErrors(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkErrors(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query network errors", err)
		return
	}
	RespondOK(c, resp)
}

func (h *NetworkHandler) GetNetworkDropped(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkDropped(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query dropped packets", err)
		return
	}
	RespondOK(c, resp)
}

func (h *NetworkHandler) GetNetworkConnections(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetNetworkConnections(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query network connections", err)
		return
	}
	RespondOK(c, resp)
}
