package redmetrics

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// REDMetricsHandler handles RED metrics endpoints.
type REDMetricsHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetTopSlowOperations returns operations ranked by p99 latency.
func (h *REDMetricsHandler) GetTopSlowOperations(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	limit := ParseIntParam(c, "limit", 20)

	ops, err := h.Service.GetTopSlowOperations(teamID, startMs, endMs, limit)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query top slow operations")
		return
	}
	RespondOK(c, ops)
}

// GetTopErrorOperations returns operations ranked by error rate.
func (h *REDMetricsHandler) GetTopErrorOperations(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	limit := ParseIntParam(c, "limit", 20)

	ops, err := h.Service.GetTopErrorOperations(teamID, startMs, endMs, limit)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query top error operations")
		return
	}
	RespondOK(c, ops)
}

// GetHTTPStatusDistribution returns span counts grouped by HTTP status code.
func (h *REDMetricsHandler) GetHTTPStatusDistribution(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	buckets, timeseries, err := h.Service.GetHTTPStatusDistribution(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query HTTP status distribution")
		return
	}
	RespondOK(c, map[string]any{
		"buckets":    buckets,
		"timeseries": timeseries,
	})
}

// GetServiceScorecard returns per-service RPS, error%, p95 stat tiles.
func (h *REDMetricsHandler) GetServiceScorecard(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	scorecard, err := h.Service.GetServiceScorecard(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query service scorecard")
		return
	}
	RespondOK(c, scorecard)
}

// GetApdex returns per-service Apdex scores.
// Query params: satisfied_ms (default 300), tolerating_ms (default 1200).
func (h *REDMetricsHandler) GetApdex(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	satisfiedMs := parseFloatParam(c, "satisfied_ms", 300.0)
	toleratingMs := parseFloatParam(c, "tolerating_ms", 1200.0)

	scores, err := h.Service.GetApdex(teamID, startMs, endMs, satisfiedMs, toleratingMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query Apdex scores")
		return
	}
	RespondOK(c, scores)
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
