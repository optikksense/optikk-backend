package metrics

import (
	. "github.com/observability/observability-backend-go/internal/platform/handlers"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// GetDashboardOverview — combined metrics / logs / traces overview for the last hour.
func (h *MetricHandler) GetDashboardOverview(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	end := time.Now().UTC()
	start := end.Add(-1 * time.Hour)

	serviceMetrics, logsData, tracesData, err := h.Repo.GetDashboardOverview(teamUUID, start, end)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query dashboard overview")
		return
	}

	totalRequests := int64(0)
	totalLatency := 0.0
	for _, row := range serviceMetrics {
		totalRequests += Int64FromAny(row["request_count"])
		totalLatency += Float64FromAny(row["avg_latency"])
	}
	avgLatency := 0.0
	if len(serviceMetrics) > 0 {
		avgLatency = totalLatency / float64(len(serviceMetrics))
	}

	levelCounts := map[string]int64{}
	for _, logRow := range logsData {
		levelCounts[StringFromAny(logRow["level"])]++
	}

	errorTraces := int64(0)
	for _, t := range tracesData {
		if strings.EqualFold(StringFromAny(t["status"]), "ERROR") {
			errorTraces++
		}
	}
	statusCounts := map[string]int64{
		"OK":    int64(len(tracesData)) - errorTraces,
		"ERROR": errorTraces,
	}

	RespondOK(c, map[string]any{
		"metrics": map[string]any{
			"count":      len(serviceMetrics),
			"recent":     []any{},
			"statistics": map[string]any{"avgLatency": avgLatency, "totalRequests": totalRequests},
		},
		"logs": map[string]any{
			"count":       len(logsData),
			"recent":      []any{},
			"levelCounts": levelCounts,
		},
		"traces": map[string]any{
			"count":        len(tracesData),
			"recent":       []any{},
			"statusCounts": statusCounts,
		},
		"timeRange": map[string]any{
			"start": start.UnixMilli(),
			"end":   end.UnixMilli(),
		},
	})
}

// GetDashboardServices — service list with health status for the last hour.
func (h *MetricHandler) GetDashboardServices(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	end := time.Now().UTC()
	start := end.Add(-1 * time.Hour)
	rows, err := h.Repo.GetDashboardServices(teamUUID, start, end)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query services")
		return
	}

	services := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		requestCount := Int64FromAny(r["request_count"])
		errorCount := Int64FromAny(r["error_count"])
		errorRate := calcErrorRate(requestCount, errorCount)
		services = append(services, map[string]any{
			"name":        r["service_name"],
			"status":      dashboardServiceStatus(errorRate),
			"metricCount": requestCount,
			"logCount":    int64(0),
			"traceCount":  requestCount,
			"errorCount":  errorCount,
			"errorRate":   errorRate,
			"lastSeen":    time.Now().UnixMilli(),
		})
	}
	RespondOK(c, services)
}

// GetDashboardServiceDetail — single service health card.
func (h *MetricHandler) GetDashboardServiceDetail(c *gin.Context) {
	serviceName := c.Param("serviceName")
	teamUUID := h.GetTenant(c).TeamUUID()
	end := time.Now().UTC()
	start := end.Add(-1 * time.Hour)
	row, err := h.Repo.GetDashboardServiceDetail(teamUUID, serviceName, start, end)
	if err != nil || len(row) == 0 {
		RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "Service not found")
		return
	}
	requestCount := Int64FromAny(row["request_count"])
	errorCount := Int64FromAny(row["error_count"])
	errorRate := calcErrorRate(requestCount, errorCount)
	RespondOK(c, map[string]any{
		"name":        row["service_name"],
		"status":      dashboardServiceStatus(errorRate),
		"metricCount": requestCount,
		"logCount":    int64(0),
		"traceCount":  requestCount,
		"errorCount":  errorCount,
		"errorRate":   errorRate,
		"lastSeen":    time.Now().UnixMilli(),
	})
}

// GetSystemStatus — basic liveness / version endpoint.
func (h *MetricHandler) GetSystemStatus(c *gin.Context) {
	RespondOK(c, map[string]any{
		"version": "v2-go",
		"tables":  []string{"spans", "logs", "incidents", "metrics", "deployments", "health_check_results"},
	})
}
