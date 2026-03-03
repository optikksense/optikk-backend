package saturation

import (
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// ClickHouseRepository encapsulates all saturation data-access logic.
// Domain-specific methods live in repository_kafka.go and repository_database.go.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new saturation ClickHouseRepository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// ──────────────────────────────────────────────────────────────────────────────
// Shared SQL aggregate helpers
// ──────────────────────────────────────────────────────────────────────────────

// syncAggregateExpr wraps N SQL expressions in a ClickHouse array average that
// ignores NULL values. If all inputs are NULL the expression returns NULL,
// letting callers decide on a fallback (e.g. coalesce(..., 0)).
//
// Example usage:
//
//	syncAggregateExpr(
//	    `avgIf(avg, metric_name = 'mongodb.driver.commands' AND isFinite(avg)) * 1000`,
//	    `avgIf(avg, metric_name = 'hikaricp.connections.usage' AND isFinite(avg)) * 1000`,
//	)
func syncAggregateExpr(parts ...string) string {
	joined := strings.Join(parts, ", ")
	return `if(
		length(arrayFilter(x -> isNotNull(x), [` + joined + `])) > 0,
		arrayReduce('avg', arrayFilter(x -> isNotNull(x), [` + joined + `])),
		NULL
	)`
}

// ──────────────────────────────────────────────────────────────────────────────
// Shared scan-layer helpers
// ──────────────────────────────────────────────────────────────────────────────

// mergeNullableFloatPair averages two nullable float64 pointers.
// Returns 0 only when both inputs are nil.
func mergeNullableFloatPair(a, b *float64) float64 {
	if a != nil && b != nil {
		return (*a + *b) / 2
	}
	if a != nil {
		return *a
	}
	if b != nil {
		return *b
	}
	return 0
}

// nullableMergedFloatPair is the pointer-returning variant of mergeNullableFloatPair.
// Returns nil when both inputs are nil, preserving the nullable semantics
// expected by JSON omitempty / frontend null checks.
func nullableMergedFloatPair(a, b *float64) *float64 {
	if a == nil && b == nil {
		return nil
	}
	v := mergeNullableFloatPair(a, b)
	return &v
}
