package overview

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler handles HTTP requests for the AI overview module.
type Handler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *Handler) parseFilter(c *gin.Context) (OverviewFilter, bool) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return OverviewFilter{}, false
	}
	return OverviewFilter{
		TeamID:    teamID,
		StartMs:   startMs,
		EndMs:     endMs,
		Service:   c.Query("service"),
		Model:     c.Query("model"),
		Operation: c.Query("operation"),
		Provider:  c.Query("provider"),
	}, true
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

func (h *Handler) GetModels(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetModels(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI models", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetOperations(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetOperations(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI operations", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetServices(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetServices(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI services", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetModelHealth(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetModelHealth(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query model health", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetTopSlow(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	limit := modulecommon.ParseIntParam(c, "limit", 10)
	resp, err := h.Service.GetTopSlow(f, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top slow operations", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetTopErrors(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	limit := modulecommon.ParseIntParam(c, "limit", 10)
	resp, err := h.Service.GetTopErrors(f, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top error operations", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetFinishReasons(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetFinishReasons(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query finish reasons", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetTimeseriesRequests(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTimeseriesRequests(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query request timeseries", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetTimeseriesLatency(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTimeseriesLatency(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency timeseries", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetTimeseriesErrors(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTimeseriesErrors(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error timeseries", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetTimeseriesTokens(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTimeseriesTokens(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query token timeseries", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetTimeseriesThroughput(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTimeseriesThroughput(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query throughput timeseries", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetTimeseriesCost(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTimeseriesCost(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query cost timeseries", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
