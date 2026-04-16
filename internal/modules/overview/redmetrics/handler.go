package redmetrics

import (
	"net/http"

	errorcode "github.com/Optikk-Org/optikk-backend/internal/shared/contracts"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type REDMetricsHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *REDMetricsHandler) GetSummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSummary(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query RED summary", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetApdex(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	satisfiedMs := modulecommon.ParseFloatParam(c, "satisfied_ms", 300.0)
	toleratingMs := modulecommon.ParseFloatParam(c, "tolerating_ms", 1200.0)
	if satisfiedMs <= 0 || toleratingMs <= 0 || satisfiedMs >= toleratingMs {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "satisfied_ms must be positive and less than tolerating_ms")
		return
	}
	resp, err := h.Service.GetApdex(c.Request.Context(), teamID, startMs, endMs, satisfiedMs, toleratingMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query Apdex scores", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetTopSlowOperations(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	limit := modulecommon.ParseIntParam(c, "limit", 20)
	resp, err := h.Service.GetTopSlowOperations(c.Request.Context(), teamID, startMs, endMs, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top slow operations", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetTopErrorOperations(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	limit := modulecommon.ParseIntParam(c, "limit", 20)
	resp, err := h.Service.GetTopErrorOperations(c.Request.Context(), teamID, startMs, endMs, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top error operations", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetRequestRateTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := modulecommon.WithComparison(c, startMs, endMs, func(s, e int64) (any, error) {
		return h.Service.GetRequestRateTimeSeries(c.Request.Context(), teamID, s, e)
	})
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query request rate time series", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetErrorRateTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := modulecommon.WithComparison(c, startMs, endMs, func(s, e int64) (any, error) {
		return h.Service.GetErrorRateTimeSeries(c.Request.Context(), teamID, s, e)
	})
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error rate time series", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetP95LatencyTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := modulecommon.WithComparison(c, startMs, endMs, func(s, e int64) (any, error) {
		return h.Service.GetP95LatencyTimeSeries(c.Request.Context(), teamID, s, e)
	})
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query p95 latency time series", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetSpanKindBreakdown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSpanKindBreakdown(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span kind breakdown", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetErrorsByRoute(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorsByRoute(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query errors by route", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetLatencyBreakdown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyBreakdown(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency breakdown", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
