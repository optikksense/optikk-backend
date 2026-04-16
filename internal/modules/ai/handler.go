package ai

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/analytics"
	errorcode "github.com/Optikk-Org/optikk-backend/internal/shared/contracts"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service Service
}

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

func (h *Handler) QueryRuns(c *gin.Context) {
	var req QueryRequest
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

func (h *Handler) Analytics(c *gin.Context) {
	var req analytics.AnalyticsRequest
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

func (h *Handler) GetRunDetail(c *gin.Context) {
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

func (h *Handler) GetRunRelated(c *gin.Context) {
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
