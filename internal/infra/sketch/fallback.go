package sketch

import "context"

// Fallback is the escape hatch for cold starts and Redis outages: when the
// sketch store returns no data for a (kind, team, range), Querier delegates
// to Fallback, which typically runs a narrow ClickHouse query with
// quantileTDigest / uniq.
//
// Implementations live outside this package (they need access to the CH
// connection and the tenant's query budget). A stub NoFallback is provided
// for tests.
type Fallback interface {
	Quantiles(ctx context.Context, kind Kind, teamID string, startMs, endMs int64, qs ...float64) (map[string][]float64, error)
	Uniques(ctx context.Context, kind Kind, teamID string, startMs, endMs int64) (map[string]uint64, error)
}

// NoFallback always returns nil, nil. Used when fallback is genuinely
// undesirable (e.g. in unit tests that assert Redis behavior in isolation).
type NoFallback struct{}

func (NoFallback) Quantiles(ctx context.Context, kind Kind, teamID string, startMs, endMs int64, qs ...float64) (map[string][]float64, error) {
	return map[string][]float64{}, nil
}

func (NoFallback) Uniques(ctx context.Context, kind Kind, teamID string, startMs, endMs int64) (map[string]uint64, error) {
	return map[string]uint64{}, nil
}
