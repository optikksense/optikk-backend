package overview

import (
	"net/http"

	errorcode "github.com/Optikk-Org/optikk-backend/internal/shared/contracts"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler exposes the AI overview HTTP endpoints.
type Handler struct {
	modulecommon.DBTenant
	Service Service
}

// NewHandler creates a new overview handler.
func NewHandler(getTenant modulecommon.GetTenantFunc, service Service) *Handler {
	return &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  service,
	}
}

func (h *Handler) GetOverview(c *gin.Context) {
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetOverview(c.Request.Context(), h.GetTenant(c).TeamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load AI overview", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetOverviewTimeseries(c *gin.Context) {
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetOverviewTimeseries(c.Request.Context(), h.GetTenant(c).TeamID, startMs, endMs, c.Query("step"))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load AI trends", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetTopModels(c *gin.Context) {
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTopModels(c.Request.Context(), h.GetTenant(c).TeamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load top models", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetTopPrompts(c *gin.Context) {
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTopPrompts(c.Request.Context(), h.GetTenant(c).TeamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load top prompts", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetQualitySummary(c *gin.Context) {
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetQualitySummary(c.Request.Context(), h.GetTenant(c).TeamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load AI quality summary", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
