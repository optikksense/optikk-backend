package redmetrics

import (
	"net/http"
	"strconv"

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
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query RED summary")
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
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query service scorecard")
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
	satisfiedMs := parseFloatParam(c, "satisfied_ms", 300.0)
	toleratingMs := parseFloatParam(c, "tolerating_ms", 1200.0)
	resp, err := h.Service.GetApdex(teamID, startMs, endMs, satisfiedMs, toleratingMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Apdex scores")
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
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query HTTP status distribution")
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
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query top slow operations")
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
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query top error operations")
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
	resp, err := h.Service.GetRequestRateTimeSeries(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query request rate time series")
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
	resp, err := h.Service.GetErrorRateTimeSeries(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error rate time series")
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
	resp, err := h.Service.GetP95LatencyTimeSeries(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query p95 latency time series")
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
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query span kind breakdown")
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
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query errors by route")
		return
	}
	RespondOK(c, resp)
}

func parseFloatParam(c *gin.Context, key string, defaultVal float64) float64 {
	s := c.Query(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return defaultVal
	}
	return v
}
