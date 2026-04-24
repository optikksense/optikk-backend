package collection

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetCollectionLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	collection, ok := shared.RequireCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionLatency(c.Request.Context(), teamID, startMs, endMs, collection, shared.ParseFilters(c))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query collection latency", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetCollectionOps(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	collection, ok := shared.RequireCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionOps(c.Request.Context(), teamID, startMs, endMs, collection, shared.ParseFilters(c))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query collection ops", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetCollectionErrors(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	collection, ok := shared.RequireCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionErrors(c.Request.Context(), teamID, startMs, endMs, collection, shared.ParseFilters(c))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query collection errors", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetCollectionQueryTexts(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	collection, ok := shared.RequireCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionQueryTexts(c.Request.Context(), teamID, startMs, endMs, collection, shared.ParseFilters(c), shared.ParseLimit(c, 10))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query collection query texts", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetCollectionReadVsWrite(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	collection, ok := shared.RequireCollection(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCollectionReadVsWrite(c.Request.Context(), teamID, startMs, endMs, collection)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query collection read vs write", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
