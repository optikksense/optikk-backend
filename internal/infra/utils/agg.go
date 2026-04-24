package utils

import "math"

// AvgNonNull averages every non-nil, finite, non-negative entry.
// Returns (mean, true) when at least one entry qualified; (0, false) otherwise.
// Replaces the ClickHouse idiom:
//
//	arrayReduce('avg', arrayFilter(x -> isNotNull(x), [c1, c2, ...]))
func AvgNonNull(vals ...*float64) (float64, bool) {
	var sum float64
	var n int
	for _, p := range vals {
		if p == nil {
			continue
		}
		v := *p
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			continue
		}
		sum += v
		n++
	}
	if n == 0 {
		return 0, false
	}
	return sum / float64(n), true
}

// MaxNonNull returns the largest non-nil, finite entry.
func MaxNonNull(vals ...*float64) (float64, bool) {
	found := false
	var max float64
	for _, p := range vals {
		if p == nil {
			continue
		}
		v := *p
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		if !found || v > max {
			max = v
			found = true
		}
	}
	return max, found
}

// AvgNonNullPtr wraps AvgNonNull to return a *float64 (nil when no entries
// qualified). Useful where the downstream DTO stores a nullable value.
func AvgNonNullPtr(vals ...*float64) *float64 {
	v, ok := AvgNonNull(vals...)
	if !ok {
		return nil
	}
	return &v
}
