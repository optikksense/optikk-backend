package explorer

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler handles HTTP requests for the AI explorer module.
type Handler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *Handler) parseFilter(c *gin.Context) (ExplorerFilter, bool) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return ExplorerFilter{}, false
	}
	return ExplorerFilter{
		TeamID:        teamID,
		StartMs:       startMs,
		EndMs:         endMs,
		Service:       c.Query("service"),
		Model:         c.Query("model"),
		Provider:      c.Query("provider"),
		Operation:     c.Query("operation"),
		Status:        c.Query("status"),
		FinishReason:  c.Query("finishReason"),
		MinDurationMs: modulecommon.ParseFloatParam(c, "minDurationMs", 0),
		MaxDurationMs: modulecommon.ParseFloatParam(c, "maxDurationMs", 0),
		TraceID:       c.Query("traceId"),
		Limit:         modulecommon.ParseIntParam(c, "limit", 50),
		Offset:        modulecommon.ParseIntParam(c, "offset", 0),
		Sort:          c.DefaultQuery("sort", "timestamp"),
		SortDir:       c.DefaultQuery("sortDir", "desc"),
	}, true
}

func (h *Handler) GetSpans(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSpans(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI spans", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetFacets(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetFacets(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI facets", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetSummary(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSummary(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI summary", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetHistogram(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetHistogram(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI histogram", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
