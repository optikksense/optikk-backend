// Package rollup centralizes CH rollup-tier selection for aggregate read paths.
//
// Every repository that reads a Phase-5 or Phase-6 AggregatingMergeTree rollup
// calls TierTableFor with its table prefix + the caller's time range. The
// helper returns the coarsest tier whose bucket count stays below the CH
// "cheap scan" threshold (~1k–2k buckets), matching the cascade MV chain in
// db/clickhouse/ (1m → 5m → 1h).
package rollup

// StepMinutes is the bucket width of a tier, in minutes. Callers use it to
// group by `toStartOfInterval(bucket_ts, toIntervalMinute(@stepMin))` when
// they want a coarser-than-tier-native time step. For the native step (e.g.
// the 1h tier's own 60-minute buckets) callers can bind @stepMin as-is.
type StepMinutes int64

// Tier names a rollup granularity. Exposed for callers that want to log or
// branch on the choice.
type Tier struct {
	Suffix   string      // "_1m" / "_5m" / "_1h"
	StepMin  StepMinutes // 1 / 5 / 60
}

var (
	Tier1m = Tier{Suffix: "_1m", StepMin: 1}
	Tier5m = Tier{Suffix: "_5m", StepMin: 5}
	Tier1h = Tier{Suffix: "_1h", StepMin: 60}
)

// PickTier returns the coarsest tier that still gives good resolution for the
// requested range. Thresholds match the cascade MV chain:
//   - ≤ 3 h  → 1m (≤ 180 buckets native)
//   - ≤ 24 h → 5m (≤ 288 buckets)
//   - > 24 h → 1h (up to 2160 buckets at 90-day TTL)
func PickTier(startMs, endMs int64) Tier {
	spanMs := endMs - startMs
	const hourMs = int64(3_600_000)
	switch {
	case spanMs <= 3*hourMs:
		return Tier1m
	case spanMs <= 24*hourMs:
		return Tier5m
	default:
		return Tier1h
	}
}

// TierTableFor picks a rollup tier for the given time range and returns the
// fully-qualified CH table name plus the tier's native step (minutes).
//
// Example:
//
//	table, step := rollup.TierTableFor("observability.spans_rollup", startMs, endMs)
//	// → "observability.spans_rollup_5m", 5   (for a 12h range)
//
// Callers that want to re-aggregate to a coarser step than the tier's native
// step pass @stepMin = max(tier.StepMin, desiredStepMin) and call
// toStartOfInterval at query time.
func TierTableFor(tablePrefix string, startMs, endMs int64) (table string, stepMin int64) {
	t := PickTier(startMs, endMs)
	return tablePrefix + t.Suffix, int64(t.StepMin)
}
