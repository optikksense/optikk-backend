package filter

import (
	"cmp"
	"slices"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)


// FormatTime is the canonical wire format for kafka panel timestamps.
func FormatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}

// CounterRateFold is one (display_bucket, dim, rate-per-sec) tuple emitted by
// FoldCounterRateByDim.
type CounterRateFold struct {
	Ts   time.Time
	Dim  string
	Rate float64
}

// FoldCounterRateByDim sums values per (display_bucket, dim) and divides
// by the bucket-grain seconds to produce a rate.
func FoldCounterRateByDim[R any](rows []R, tsOf func(R) time.Time, dimOf func(R) string, valOf func(R) float64, startMs, endMs int64) []CounterRateFold {
	type key struct {
		ts  time.Time
		dim string
	}
	sums := map[key]float64{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(tsOf(r).Unix(), windowMs), dim: dimOf(r)}
		sums[k] += valOf(r)
	}
	bucketSecs := timebucket.DisplayGrain(windowMs).Seconds()
	if bucketSecs <= 0 {
		bucketSecs = 1
	}
	out := make([]CounterRateFold, 0, len(sums))
	for k, v := range sums {
		out = append(out, CounterRateFold{Ts: k.ts, Dim: k.dim, Rate: v / bucketSecs})
	}
	slices.SortFunc(out, func(a, b CounterRateFold) int {
		if c := a.Ts.Compare(b.Ts); c != 0 {
			return c
		}
		return cmp.Compare(a.Dim, b.Dim)
	})
	return out
}


