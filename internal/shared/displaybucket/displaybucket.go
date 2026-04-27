// Package displaybucket folds raw time-series rows into display-time buckets keyed by timebucket.DisplayBucket.
package displaybucket

import (
	"sort"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

type TimeBucket struct {
	Timestamp string   `json:"timestamp"`
	Value     *float64 `json:"value"`
}

type StateBucket struct {
	Timestamp string   `json:"timestamp"`
	State     string   `json:"state"`
	Value     *float64 `json:"value"`
}

// SumByTime sums val(row) into display buckets — used for counters/rates where bucket value = total.
func SumByTime[R any](rows []R, ts func(R) time.Time, val func(R) float64, startMs, endMs int64) []TimeBucket {
	windowMs := endMs - startMs
	sums := map[time.Time]float64{}
	for _, r := range rows {
		key := timebucket.DisplayBucket(ts(r).Unix(), windowMs)
		sums[key] += val(r)
	}
	out := make([]TimeBucket, 0, len(sums))
	for tt, sum := range sums {
		v := sum
		out = append(out, TimeBucket{Timestamp: format(tt), Value: &v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	return out
}

// AvgByTime averages val(row) within each display bucket — used for gauges.
func AvgByTime[R any](rows []R, ts func(R) time.Time, val func(R) float64, startMs, endMs int64) []TimeBucket {
	windowMs := endMs - startMs
	type acc struct {
		sum   float64
		count int
	}
	a := map[time.Time]*acc{}
	for _, r := range rows {
		key := timebucket.DisplayBucket(ts(r).Unix(), windowMs)
		x, ok := a[key]
		if !ok {
			x = &acc{}
			a[key] = x
		}
		x.sum += val(r)
		x.count++
	}
	out := make([]TimeBucket, 0, len(a))
	for tt, x := range a {
		var vp *float64
		if x.count > 0 {
			v := x.sum / float64(x.count)
			vp = &v
		}
		out = append(out, TimeBucket{Timestamp: format(tt), Value: vp})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	return out
}

// AvgByTimeAndKey averages val(row) within each (display bucket, key(row)) pair — used for state/dim breakdowns.
func AvgByTimeAndKey[R any](rows []R, ts func(R) time.Time, key func(R) string, val func(R) float64, startMs, endMs int64) []StateBucket {
	windowMs := endMs - startMs
	type k struct {
		ts    time.Time
		state string
	}
	type acc struct {
		sum   float64
		count int
	}
	a := map[k]*acc{}
	for _, r := range rows {
		kk := k{ts: timebucket.DisplayBucket(ts(r).Unix(), windowMs), state: key(r)}
		x, ok := a[kk]
		if !ok {
			x = &acc{}
			a[kk] = x
		}
		x.sum += val(r)
		x.count++
	}
	out := make([]StateBucket, 0, len(a))
	for kk, x := range a {
		var vp *float64
		if x.count > 0 {
			v := x.sum / float64(x.count)
			vp = &v
		}
		out = append(out, StateBucket{Timestamp: format(kk.ts), State: kk.state, Value: vp})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Timestamp != out[j].Timestamp {
			return out[i].Timestamp < out[j].Timestamp
		}
		return out[i].State < out[j].State
	})
	return out
}

func format(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}
