package redmetrics

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type REDMetricsHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *REDMetricsHandler) GetSummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSummary(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query RED summary", err)
		return
	}
	RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetServiceScorecard(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetServiceScorecard(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query service scorecard", err)
		return
	}
	RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetApdex(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	satisfiedMs := ParseFloatParam(c, "satisfied_ms", 300.0)
	toleratingMs := ParseFloatParam(c, "tolerating_ms", 1200.0)
	if satisfiedMs <= 0 || toleratingMs <= 0 || satisfiedMs >= toleratingMs {
		RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "satisfied_ms must be positive and less than tolerating_ms")
		return
	}
	resp, err := h.Service.GetApdex(teamID, startMs, endMs, satisfiedMs, toleratingMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query Apdex scores", err)
		return
	}
	RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetHTTPStatusDistribution(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetHTTPStatusDistribution(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query HTTP status distribution", err)
		return
	}
	RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetTopSlowOperations(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	limit := ParseIntParam(c, "limit", 20)
	resp, err := h.Service.GetTopSlowOperations(teamID, startMs, endMs, limit)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top slow operations", err)
		return
	}
	RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetTopErrorOperations(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	limit := ParseIntParam(c, "limit", 20)
	resp, err := h.Service.GetTopErrorOperations(teamID, startMs, endMs, limit)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top error operations", err)
		return
	}
	RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetRequestRateTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := WithComparison(c, startMs, endMs, func(s, e int64) (any, error) {
		return h.Service.GetRequestRateTimeSeries(teamID, s, e)
	})
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query request rate time series", err)
		return
	}
	RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetErrorRateTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := WithComparison(c, startMs, endMs, func(s, e int64) (any, error) {
		return h.Service.GetErrorRateTimeSeries(teamID, s, e)
	})
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error rate time series", err)
		return
	}
	RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetP95LatencyTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := WithComparison(c, startMs, endMs, func(s, e int64) (any, error) {
		return h.Service.GetP95LatencyTimeSeries(teamID, s, e)
	})
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query p95 latency time series", err)
		return
	}
	RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetSpanKindBreakdown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSpanKindBreakdown(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span kind breakdown", err)
		return
	}
	RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetErrorsByRoute(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorsByRoute(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query errors by route", err)
		return
	}
	RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetLatencyBreakdown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyBreakdown(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency breakdown", err)
		return
	}
	RespondOK(c, resp)
}
