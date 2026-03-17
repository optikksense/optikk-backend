package collection

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

func (h *Handler) GetCollectionLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	collection, ok := shared.RequireCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionLatency(c.Request.Context(), teamID, startMs, endMs, collection, shared.ParseFilters(c))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query collection latency")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetCollectionOps(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	collection, ok := shared.RequireCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionOps(c.Request.Context(), teamID, startMs, endMs, collection, shared.ParseFilters(c))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query collection ops")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetCollectionErrors(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	collection, ok := shared.RequireCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionErrors(c.Request.Context(), teamID, startMs, endMs, collection, shared.ParseFilters(c))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query collection errors")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetCollectionQueryTexts(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	collection, ok := shared.RequireCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionQueryTexts(c.Request.Context(), teamID, startMs, endMs, collection, shared.ParseFilters(c), shared.ParseLimit(c, 10))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query collection query texts")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetCollectionReadVsWrite(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	collection, ok := shared.RequireCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionReadVsWrite(c.Request.Context(), teamID, startMs, endMs, collection)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query collection read vs write")
		return
	}
	common.RespondOK(c, resp)
}
