// Package rollup centralises ClickHouse rollup-tier selection and bucket-
// interval helpers used by every repository that reads AggregatingMergeTree
// rollups.
//
// Every family (see families.go) has three tiers: _1m, _5m, _1h. TTLs are:
//
//	_1m → 72 hours   (live dashboards, recent drilldown)
//	_5m → 14 days    (medium-range historical)
//	_1h → 90 days    (long-range historical)
//
// PickTier is TTL-aware: it returns the finest tier whose retention covers
// the earliest timestamp in the query window. Callers go through For() which
// returns everything a reader needs (table name, native step, bucket
// expression string) so nobody reimplements interval arithmetic.
package rollup

import "time"

// TierLevel enumerates the three cascade tiers.
type TierLevel int

const (
	Tier1m TierLevel = iota
	Tier5m
	Tier1h
)

// Suffix returns the ClickHouse table suffix for the tier ("_1m", "_5m",
// "_1h"). Appended to a family name plus the `observability.` database
// qualifier to form a fully-qualified table name.
func (l TierLevel) Suffix() string {
	switch l {
	case Tier1m:
		return "_1m"
	case Tier5m:
		return "_5m"
	default:
		return "_1h"
	}
}

// StepMinutes returns the native bucket width for the tier.
func (l TierLevel) StepMinutes() int64 {
	switch l {
	case Tier1m:
		return 1
	case Tier5m:
		return 5
	default:
		return 60
	}
}

// Tier bundles everything a reader needs to form a rollup query.
type Tier struct {
	Level      TierLevel
	Table      string // e.g. "observability.spans_red_5m"
	StepMin    int64  // 1 / 5 / 60
	BucketExpr string // "toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin))"
}

// Tier-TTL bounds. Keep in sync with the TTL clauses in db/clickhouse/*.sql.
const (
	tier1mTTL = 72 * time.Hour
	tier5mTTL = 14 * 24 * time.Hour
)

// PickTier returns the finest tier whose retention still covers startMs.
// Window width no longer factors in — TTL coverage is the binding constraint.
// If the data is gone past the 1h TTL too, 1h is still returned and the
// reader gets an empty result (acceptable; UI surfaces it as "no data").
func PickTier(now time.Time, startMs int64) TierLevel {
	start := time.UnixMilli(startMs)
	age := now.Sub(start)
	switch {
	case age <= tier1mTTL:
		return Tier1m
	case age <= tier5mTTL:
		return Tier5m
	default:
		return Tier1h
	}
}

// bucketExpr is the one canonical bucket expression. Every reader's SELECT
// should use this verbatim and bind `@intervalMin` to BucketInterval(tier,
// uiStepMinutes).
const bucketExpr = "toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin))"

// For picks a tier using the process clock and returns a fully-populated
// Tier. Tests should prefer ForAt for clock injection.
func For(family string, startMs, endMs int64) Tier {
	return ForAt(time.Now(), family, startMs, endMs)
}

// ForAt is the test-friendly variant of For.
func ForAt(now time.Time, family string, startMs, endMs int64) Tier {
	level := PickTier(now, startMs)
	return Tier{
		Level:      level,
		Table:      "observability." + family + level.Suffix(),
		StepMin:    level.StepMinutes(),
		BucketExpr: bucketExpr,
	}
}
