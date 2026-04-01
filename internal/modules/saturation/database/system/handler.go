package system

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetSystemLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	dbSystem, ok := shared.RequireDBSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemLatency(c.Request.Context(), teamID, startMs, endMs, dbSystem, shared.ParseFilters(c))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query system latency", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetSystemOps(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	dbSystem, ok := shared.RequireDBSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemOps(c.Request.Context(), teamID, startMs, endMs, dbSystem, shared.ParseFilters(c))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query system ops", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetSystemTopCollectionsByLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	dbSystem, ok := shared.RequireDBSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemTopCollectionsByLatency(c.Request.Context(), teamID, startMs, endMs, dbSystem)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top collections by latency", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetSystemTopCollectionsByVolume(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	dbSystem, ok := shared.RequireDBSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemTopCollectionsByVolume(c.Request.Context(), teamID, startMs, endMs, dbSystem)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top collections by volume", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetSystemErrors(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	dbSystem, ok := shared.RequireDBSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemErrors(c.Request.Context(), teamID, startMs, endMs, dbSystem)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query system errors", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetSystemNamespaces(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	dbSystem, ok := shared.RequireDBSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemNamespaces(c.Request.Context(), teamID, startMs, endMs, dbSystem)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query system namespaces", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
