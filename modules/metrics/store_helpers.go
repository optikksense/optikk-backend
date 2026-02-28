package metrics

import (
	"fmt"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// ClickHouseRepository is the data access layer for metrics.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new metrics repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// serviceStatus derives a health status from error rate.
func serviceStatus(errorRate float64) string {
	if errorRate > 10 {
		return "CRITICAL"
	}
	if errorRate > 1 {
		return "WARNING"
	}
	return "HEALTHY"
}

// errorRate computes error rate percentage given request and error counts.
func errorRate(requestCount, errorCount int64) float64 {
	if requestCount == 0 {
		return 0
	}
	return float64(errorCount) * 100.0 / float64(requestCount)
}

// selectServiceView picks the materialized view based on the query window.
// At petabyte scale, wider windows use coarser rollups (once hourly/daily
// views are created). For now falls back to the 1-minute view.
func selectServiceView(start, end time.Time) string {
	_ = end.Sub(start)
	// TODO: once spans_service_1h / spans_service_1d views exist:
	// window > 7d → spans_service_1d
	// window > 6h → spans_service_1h
	return "observability.spans_service_1m"
}

// selectEndpointView picks the endpoint materialized view.
func selectEndpointView(start, end time.Time) string {
	_ = end.Sub(start)
	return "observability.spans_endpoint_1m"
}

// autoStep picks a bucket size so the query window produces ~20-30 data points
// instead of returning one row per minute. For a 1h window this returns "1m"
// (60 points); for a 30d window it returns "1d" (30 points).
func autoStep(start, end time.Time) string {
	ms := end.Sub(start).Milliseconds()
	targetBars := int64(30)
	stepMs := ms / targetBars

	switch {
	case stepMs <= 1*60*1000:
		return "1m"
	case stepMs <= 2*60*1000:
		return "2m"
	case stepMs <= 5*60*1000:
		return "5m"
	case stepMs <= 10*60*1000:
		return "10m"
	case stepMs <= 15*60*1000:
		return "15m"
	case stepMs <= 30*60*1000:
		return "30m"
	case stepMs <= 60*60*1000:
		return "1h"
	case stepMs <= 2*60*60*1000:
		return "2h"
	case stepMs <= 6*60*60*1000:
		return "6h"
	case stepMs <= 12*60*60*1000:
		return "12h"
	default:
		return "1d"
	}
}

// metricBucketExpr returns the ClickHouse SQL expression that re-buckets
// the 1-minute `minute` column into a coarser interval. It uses
// toStartOfInterval which works natively on DateTime columns in ClickHouse.
func metricBucketExpr(step string) string {
	switch step {
	case "1m":
		return "minute" // no re-bucketing needed, already 1-minute granularity
	case "1h":
		return "toStartOfHour(minute)"
	case "1d":
		return "toStartOfDay(minute)"
	default:
		mins := stepMinutes(step)
		return fmt.Sprintf("toStartOfInterval(minute, INTERVAL %d MINUTE)", mins)
	}
}

// stepMinutes converts a step string to its duration in minutes.
func stepMinutes(step string) int {
	switch step {
	case "1m":
		return 1
	case "2m":
		return 2
	case "5m":
		return 5
	case "10m":
		return 10
	case "15m":
		return 15
	case "30m":
		return 30
	case "1h":
		return 60
	case "2h":
		return 120
	case "6h":
		return 360
	case "12h":
		return 720
	case "1d":
		return 1440
	default:
		return 1
	}
}
