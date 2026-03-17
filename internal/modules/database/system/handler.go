package system

import (
	"net/http"

	"github.com/gin-gonic/gin"
	common "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	shared "github.com/observability/observability-backend-go/internal/modules/database/internal/shared"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetSystemLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	dbSystem, ok := shared.RequireDBSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemLatency(c.Request.Context(), teamID, startMs, endMs, dbSystem, shared.ParseFilters(c))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query system latency")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetSystemOps(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	dbSystem, ok := shared.RequireDBSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemOps(c.Request.Context(), teamID, startMs, endMs, dbSystem, shared.ParseFilters(c))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query system ops")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetSystemTopCollectionsByLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	dbSystem, ok := shared.RequireDBSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemTopCollectionsByLatency(c.Request.Context(), teamID, startMs, endMs, dbSystem)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query top collections by latency")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetSystemTopCollectionsByVolume(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	dbSystem, ok := shared.RequireDBSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemTopCollectionsByVolume(c.Request.Context(), teamID, startMs, endMs, dbSystem)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query top collections by volume")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetSystemErrors(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	dbSystem, ok := shared.RequireDBSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemErrors(c.Request.Context(), teamID, startMs, endMs, dbSystem)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query system errors")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetSystemNamespaces(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	dbSystem, ok := shared.RequireDBSystem(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSystemNamespaces(c.Request.Context(), teamID, startMs, endMs, dbSystem)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query system namespaces")
		return
	}
	common.RespondOK(c, resp)
}
