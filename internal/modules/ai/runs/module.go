// Package runs hosts the /ai/runs/* HTTP surface — run search + detail +
// related telemetry lookup. Business logic lives in the parent ai package.
package runs

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

func (h *Handler) Query(c *gin.Context) {
	var req shared.QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if req.StartTime <= 0 || req.EndTime <= 0 || req.StartTime >= req.EndTime {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Valid startTime and endTime are required")
		return
	}
	resp, err := h.Service.QueryRuns(c.Request.Context(), h.GetTenant(c).TeamID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI runs", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) Detail(c *gin.Context) {
	runID := c.Param("runId")
	if runID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "runId is required")
		return
	}
	resp, err := h.Service.GetRunDetail(c.Request.Context(), h.GetTenant(c).TeamID, runID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load AI run detail", err)
		return
	}
	if resp == nil {
		modulecommon.RespondError(c, http.StatusNotFound, errorcode.NotFound, "AI run not found")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) Related(c *gin.Context) {
	runID := c.Param("runId")
	if runID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "runId is required")
		return
	}
	resp, err := h.Service.GetRunRelated(c.Request.Context(), h.GetTenant(c).TeamID, runID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to load AI related telemetry", err)
		return
	}
	if resp == nil {
		modulecommon.RespondError(c, http.StatusNotFound, errorcode.NotFound, "AI run not found")
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

func (m *Module) Name() string                      { return "ai.runs" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *Module) RegisterRoutes(group *gin.RouterGroup) {
	group.POST("/ai/explorer/query", m.handler.Query)
	group.GET("/ai/runs/:runId", m.handler.Detail)
	group.GET("/ai/runs/:runId/related", m.handler.Related)
}

var _ registry.Module = (*Module)(nil)
