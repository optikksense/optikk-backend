// Package analytics hosts the /explorer/ai/analytics endpoint — a shared
// aggregate query surface reused by the dashboard facet + trend widgets.
package analytics

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/modules/ai/shared"
	explorer_analytics "github.com/Optikk-Org/optikk-backend/internal/modules/explorer/analytics"
	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service shared.Service
}

func (h *Handler) Analytics(c *gin.Context) {
	var req explorer_analytics.AnalyticsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if req.StartTime <= 0 || req.EndTime <= 0 || req.StartTime >= req.EndTime {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Valid startTime and endTime are required")
		return
	}
	resp, err := h.Service.RunAnalytics(c.Request.Context(), h.GetTenant(c).TeamID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "AI analytics query failed", err)
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

func (m *Module) Name() string                      { return "ai.analytics" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *Module) RegisterRoutes(group *gin.RouterGroup) {
	group.POST("/explorer/ai/analytics", m.handler.Analytics)
}

var _ registry.Module = (*Module)(nil)
