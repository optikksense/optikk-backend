package runs

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *Handler) parseFilters(c *gin.Context) (LLMRunFilters, bool) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return LLMRunFilters{}, false
	}

	f := LLMRunFilters{
		TeamID:  teamID,
		StartMs: startMs,
		EndMs:   endMs,
	}

	if v := c.Query("models"); v != "" {
		f.Models = strings.Split(v, ",")
	}
	if v := c.Query("providers"); v != "" {
		f.Providers = strings.Split(v, ",")
	}
	if v := c.Query("operations"); v != "" {
		f.Operations = strings.Split(v, ",")
	}
	if v := c.Query("services"); v != "" {
		f.Services = strings.Split(v, ",")
	}
	f.Status = c.Query("status")
	f.TraceID = c.Query("traceId")

	if v := c.Query("minDurationMs"); v != "" {
		f.MinDurationMs, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := c.Query("maxDurationMs"); v != "" {
		f.MaxDurationMs, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := c.Query("minTokens"); v != "" {
		f.MinTokens, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := c.Query("maxTokens"); v != "" {
		f.MaxTokens, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := c.Query("limit"); v != "" {
		f.Limit, _ = strconv.Atoi(v)
	}

	if ts := c.Query("cursorTimestamp"); ts != "" {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			f.CursorTimestamp = &t
		}
	}
	f.CursorSpanID = c.Query("cursorSpanId")

	return f, true
}

func (h *Handler) ListRuns(c *gin.Context) {
	f, ok := h.parseFilters(c)
	if !ok {
		return
	}
	runs, err := h.Service.ListRuns(f)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to list LLM runs")
		return
	}
	common.RespondOK(c, runs)
}

func (h *Handler) GetRunsSummary(c *gin.Context) {
	f, ok := h.parseFilters(c)
	if !ok {
		return
	}
	summary, err := h.Service.GetRunsSummary(f)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get runs summary")
		return
	}
	common.RespondOK(c, summary)
}

func (h *Handler) ListModels(c *gin.Context) {
	f, ok := h.parseFilters(c)
	if !ok {
		return
	}
	models, err := h.Service.ListModels(f)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to list models")
		return
	}
	common.RespondOK(c, models)
}

func (h *Handler) ListOperations(c *gin.Context) {
	f, ok := h.parseFilters(c)
	if !ok {
		return
	}
	ops, err := h.Service.ListOperations(f)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to list operations")
		return
	}
	common.RespondOK(c, ops)
}
