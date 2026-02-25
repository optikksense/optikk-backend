package insights

import (
	"fmt"
	"net/http"

	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"

	"github.com/gin-gonic/gin"
)

// InsightHandler handles business-insight API endpoints.
type InsightHandler struct {
	modulecommon.DBTenant
	Repo *Repository
}

// renameBuckets renames the "time_bucket" key to "timestamp" in each row
// so the frontend gets the standard key it expects.
func renameBuckets(rows []map[string]any) []map[string]any {
	for _, row := range rows {
		if v, ok := row["time_bucket"]; ok {
			row["timestamp"] = v
			delete(row, "time_bucket")
		}
	}
	return rows
}

// GetInsightResourceUtilization — CPU / memory / disk / network utilisation by service and instance.
func (h *InsightHandler) GetInsightResourceUtilization(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	byService, byInstance, infra, timeseries, err := h.Repo.GetInsightResourceUtilization(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query resource utilization")
		return
	}

	RespondOK(c, map[string]any{
		"byService":      NormalizeRows(byService),
		"byInstance":     NormalizeRows(byInstance),
		"infrastructure": NormalizeRows(infra),
		"timeseries":     renameBuckets(NormalizeRows(timeseries)),
	})
}

// GetInsightSloSli — SLO / SLI compliance status and trend.
func (h *InsightHandler) GetInsightSloSli(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs := ParseRange(c, 7*24*60*60*1000)

	summary, timeseries, err := h.Repo.GetInsightSloSli(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		fmt.Println("[SLO ERROR]", err)
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query SLO status")
		return
	}

	sanitized := NormalizeMap(summary)
	availability := Float64FromAny(sanitized["availability_percent"])
	p95 := Float64FromAny(sanitized["p95_latency_ms"])
	errorBudgetRemaining := 100.0
	if availability < 99.9 {
		errorBudgetRemaining = availability
	}

	RespondOK(c, map[string]any{
		"objectives": map[string]any{
			"availabilityTarget": 99.9,
			"p95LatencyTargetMs": 300.0,
		},
		"status": map[string]any{
			"availabilityPercent":         availability,
			"p95LatencyMs":                p95,
			"errorBudgetRemainingPercent": errorBudgetRemaining,
			"compliant":                   availability >= 99.9 && p95 <= 300.0,
		},
		"summary":    sanitized,
		"timeseries": renameBuckets(NormalizeRows(timeseries)),
	})
}

// GetInsightLogsStream — recent log stream with volume trend and correlation stats.
func (h *InsightHandler) GetInsightLogsStream(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	limit := ParseIntParam(c, "limit", 200)

	stream, total, volume, levelFacets, serviceFacets, err := h.Repo.GetInsightLogsStream(teamUUID, startMs, endMs, limit)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query logs stream")
		return
	}

	correlated := int64(0)
	for _, row := range stream {
		if StringFromAny(row["trace_id"]) != "" {
			correlated++
		}
	}
	uncorrelated := int64(len(stream)) - correlated
	if uncorrelated < 0 {
		uncorrelated = 0
	}
	ratio := 0.0
	if len(stream) > 0 {
		ratio = float64(correlated) * 100.0 / float64(len(stream))
	}

	RespondOK(c, map[string]any{
		"stream":       NormalizeRows(stream),
		"total":        total,
		"volumeTrends": renameBuckets(NormalizeRows(volume)),
		"traceCorrelation": map[string]any{
			"traceCorrelatedLogs": correlated,
			"uncorrelatedLogs":    uncorrelated,
			"correlationRatio":    ratio,
		},
		"facets": map[string]any{
			"levels":   NormalizeRows(levelFacets),
			"services": NormalizeRows(serviceFacets),
		},
	})
}

// GetInsightDatabaseCache — DB query latency and cache-hit ratio insights.
func (h *InsightHandler) GetInsightDatabaseCache(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	summary, tableMetrics, slowLogs, err := h.Repo.GetInsightDatabaseCache(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database cache insights")
		return
	}

	cacheHits := Int64FromAny(summary["cache_hits"])
	cacheMisses := Int64FromAny(summary["cache_misses"])
	total := cacheHits + cacheMisses
	hitRatio := 0.0
	if total > 0 {
		hitRatio = float64(cacheHits) * 100.0 / float64(total)
	}

	RespondOK(c, map[string]any{
		"summary":      NormalizeMap(summary),
		"tableMetrics": NormalizeRows(tableMetrics),
		"cache": map[string]any{
			"cacheHits":     cacheHits,
			"cacheMisses":   cacheMisses,
			"cacheHitRatio": hitRatio,
		},
		"slowLogs": map[string]any{
			"logs":    NormalizeRows(slowLogs),
			"hasMore": len(slowLogs) >= 50,
			"offset":  0,
			"limit":   50,
			"total":   len(slowLogs),
		},
	})
}

// GetInsightMessagingQueue — queue depth, consumer lag, and message rate insights.
func (h *InsightHandler) GetInsightMessagingQueue(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 24*60*60*1000)

	summary, timeseries, topQueues, err := h.Repo.GetInsightMessagingQueue(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query messaging insights")
		return
	}

	RespondOK(c, map[string]any{
		"summary":    NormalizeMap(summary),
		"timeseries": renameBuckets(NormalizeRows(timeseries)),
		"topQueues":  NormalizeRows(topQueues),
	})
}
