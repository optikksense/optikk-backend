package metrics

import (
	"fmt"
	"net/http"

	. "github.com/observability/observability-backend-go/internal/platform/handlers"

	"github.com/gin-gonic/gin"
)

// GetServiceMetrics — per-service request / error / latency summary.
func (h *MetricHandler) GetServiceMetrics(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, err := h.Repo.GetServiceMetrics(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query service metrics")
		return
	}
	RespondOK(c, NormalizeRows(rows))
}

// GetEndpointMetrics — per-endpoint breakdown with optional service filter.
func (h *MetricHandler) GetEndpointMetrics(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Repo.GetEndpointMetrics(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query endpoint metrics")
		return
	}
	RespondOK(c, NormalizeRows(rows))
}

// GetServiceEndpointBreakdown — endpoint metrics scoped to a path param service name.
func (h *MetricHandler) GetServiceEndpointBreakdown(c *gin.Context) {
	serviceName := c.Param("serviceName")
	raw := c.Request.URL.RawQuery
	if raw == "" {
		raw = fmt.Sprintf("serviceName=%s", serviceName)
	} else {
		raw = raw + "&serviceName=" + serviceName
	}
	c.Request.URL.RawQuery = raw
	h.GetEndpointMetrics(c)
}

// GetMetricsTimeSeries — aggregate request / error / latency over time.
func (h *MetricHandler) GetMetricsTimeSeries(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Repo.GetMetricsTimeSeries(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query metrics timeseries")
		return
	}
	// Rename time_bucket → timestamp for frontend compatibility
	for _, row := range rows {
		if v, ok := row["time_bucket"]; ok {
			row["timestamp"] = v
			delete(row, "time_bucket")
		}
	}
	RespondOK(c, NormalizeRows(rows))
}

// GetMetricsSummary — overall totals / averages for the time window.
func (h *MetricHandler) GetMetricsSummary(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	summary, err := h.Repo.GetMetricsSummary(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query metrics summary")
		return
	}
	RespondOK(c, NormalizeMap(summary))
}

// GetServiceTimeSeries — per-service request / error / latency time series from spans.
func (h *MetricHandler) GetServiceTimeSeries(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Repo.GetServiceTimeSeries(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query service timeseries")
		return
	}
	RespondOK(c, NormalizeRows(rows))
}

// GetEndpointTimeSeries — timeseries data per endpoint
func (h *MetricHandler) GetEndpointTimeSeries(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Repo.GetEndpointTimeSeries(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query endpoint timeseries")
		return
	}

	for _, row := range rows {
		if v, ok := row["time_bucket"]; ok {
			row["timestamp"] = v
			delete(row, "time_bucket")
		}
	}
	RespondOK(c, NormalizeRows(rows))
}
