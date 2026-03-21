package system

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

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
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query system latency", err)
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
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query system ops", err)
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
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top collections by latency", err)
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
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top collections by volume", err)
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
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query system errors", err)
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
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query system namespaces", err)
		return
	}
	common.RespondOK(c, resp)
}
