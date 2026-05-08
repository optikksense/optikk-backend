package filter

import (
	"cmp"
	"math"
	"slices"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// SecondsToMs converts a seconds-domain percentile (as returned by
// metrics_1m.latency_state, since OTel kafka duration metrics are
// seconds-domain) into milliseconds.
func SecondsToMs(s float64) float64 { return s * 1000.0 }

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

// FoldCounterRateByDim sums values per (display_bucket, dim), then divides by
// the bucket-grain seconds to produce a rate. Caller passes a typed row plus
// extractor closures so the helper works with any row shape.
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

// ErrorRateFold is one (display_bucket, dim, error_type, rate-per-sec) tuple
// emitted by FoldErrorRateByPair.
type ErrorRateFold struct {
	Ts        time.Time
	Dim       string
	ErrorType string
	Rate      float64
}

// FoldErrorRateByPair sums values per (bucket, dim, error_type) and divides
// by bucket-grain seconds. Used by all four error panels.
func FoldErrorRateByPair[R any](rows []R, tsOf func(R) time.Time, dimOf, errOf func(R) string, valOf func(R) float64, startMs, endMs int64) []ErrorRateFold {
	type key struct {
		ts        time.Time
		dim       string
		errorType string
	}
	sums := map[key]float64{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{
			ts:        timebucket.DisplayBucket(tsOf(r).Unix(), windowMs),
			dim:       dimOf(r),
			errorType: errOf(r),
		}
		sums[k] += valOf(r)
	}
	bucketSecs := timebucket.DisplayGrain(windowMs).Seconds()
	if bucketSecs <= 0 {
		bucketSecs = 1
	}
	out := make([]ErrorRateFold, 0, len(sums))
	for k, v := range sums {
		rate := v / bucketSecs
		if math.IsNaN(rate) {
			rate = 0
		}
		out = append(out, ErrorRateFold{Ts: k.ts, Dim: k.dim, ErrorType: k.errorType, Rate: rate})
	}
	slices.SortFunc(out, func(a, b ErrorRateFold) int {
		if c := a.Ts.Compare(b.Ts); c != 0 {
			return c
		}
		if c := cmp.Compare(b.Rate, a.Rate); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Dim, b.Dim); c != 0 {
			return c
		}
		return cmp.Compare(a.ErrorType, b.ErrorType)
	})
	return out
}
