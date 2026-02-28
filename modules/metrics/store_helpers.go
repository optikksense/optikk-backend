package metrics

import (
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
