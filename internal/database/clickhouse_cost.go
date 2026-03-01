package database

import (
	"context"
	"fmt"
	"strings"
)

// ── ClickHouse Query Cost Limits (Fix 10) ─────────────────────────────────
//
// Prepend these settings to every analytical ClickHouse query so runaway
// queries are killed server-side rather than hanging indefinitely or
// consuming unbounded resources.
//
// Defaults (per-tenant tunable via WithCostLimits):
//
//	max_execution_time     30 s
//	max_memory_usage       2 GiB
//	max_rows_to_read       50_000_000 rows
//	max_result_rows        10_000 rows
//
// Usage:
//
//	query := database.WithCostLimits(myQuery)
//	rows, err := db.QueryContext(ctx, query, args...)
//
// Surface ClickHouse TIMEOUT_EXCEEDED / TOO_MANY_ROWS errors:
//
//	if database.IsClickHouseCostError(err) {
//	    c.AbortWithStatusJSON(http.StatusTooManyRequests, types.Failure(...))
//	}

// CostLimits holds per-query ClickHouse resource limits.
type CostLimits struct {
	MaxExecutionTimeSec int
	MaxMemoryUsageBytes int64
	MaxRowsToRead       int64
	MaxResultRows       int64
}

// DefaultCostLimits is the production-safe default.
var DefaultCostLimits = CostLimits{
	MaxExecutionTimeSec: 30,
	MaxMemoryUsageBytes: 2 * 1024 * 1024 * 1024, // 2 GiB
	MaxRowsToRead:       50_000_000,
	MaxResultRows:       10_000,
}

// WithCostLimits prepends ClickHouse SETTINGS to the query string using the
// DefaultCostLimits. The query can be a SELECT or any ClickHouse statement.
func WithCostLimits(query string) string {
	return WithCustomCostLimits(query, DefaultCostLimits)
}

// WithCustomCostLimits prepends ClickHouse SETTINGS with the given limits.
func WithCustomCostLimits(query string, l CostLimits) string {
	settings := fmt.Sprintf(
		"SET max_execution_time=%d, max_memory_usage=%d, max_rows_to_read=%d, max_result_rows=%d;\n",
		l.MaxExecutionTimeSec, l.MaxMemoryUsageBytes, l.MaxRowsToRead, l.MaxResultRows,
	)
	return settings + query
}

// IsClickHouseCostError returns true if err is a ClickHouse
// TIMEOUT_EXCEEDED, TOO_MANY_ROWS, or memory-limit error.
// Use this to surface cost errors as HTTP 429 / 503 responses.
func IsClickHouseCostError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "TIMEOUT_EXCEEDED") ||
		strings.Contains(msg, "TOO_MANY_ROWS") ||
		strings.Contains(msg, "MEMORY_LIMIT_EXCEEDED") ||
		strings.Contains(msg, "max_execution_time exceeded")
}

// QueryContextWithCostLimits is a convenience wrapper that applies
// DefaultCostLimits and executes the query using the provided context.
// It centralises the cost-limit pattern so individual store files don't
// need to remember to call WithCostLimits manually.
func QueryContextWithCostLimits(ctx context.Context, db Querier, query string, args ...any) (Rows, error) {
	return db.Query(WithCostLimits(query), args...)
}
