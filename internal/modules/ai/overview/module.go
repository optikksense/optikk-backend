// Package overview hosts the /ai/overview/* HTTP surface — summary, timeseries,
// top-models, top-prompts, quality breakdown. Business logic and persistence
// live in the parent ai package (Service + Repository); this subpackage is a
// proper route-registration unit so the AI surface can be wired into
// modules_manifest as discrete modules instead of one monolithic registrar.
package overview

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/modules/ai/shared"
	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service shared.Service
}

func (h *Handler) Overview(c *gin.Context) {
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

func (h *Handler) Timeseries(c *gin.Context) {
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

func (h *Handler) TopModels(c *gin.Context) {
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

func (h *Handler) TopPrompts(c *gin.Context) {
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

func (h *Handler) Quality(c *gin.Context) {
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

type Module struct {
	handler *Handler
}

func NewModule(handler *Handler) registry.Module {
	return &Module{handler: handler}
}

func (m *Module) Name() string                      { return "ai.overview" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *Module) RegisterRoutes(group *gin.RouterGroup) {
	a := group.Group("/ai/overview")
	a.GET("", m.handler.Overview)
	a.GET("/timeseries", m.handler.Timeseries)
	a.GET("/top-models", m.handler.TopModels)
	a.GET("/top-prompts", m.handler.TopPrompts)
	a.GET("/quality", m.handler.Quality)
}

var _ registry.Module = (*Module)(nil)
