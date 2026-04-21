package utils

import (
	"math"
	"strconv"
	"time"

	"github.com/spf13/cast"
)

func Int64FromAny(v any) int64 {
	return cast.ToInt64(v)
}

func Float64FromAny(v any) float64 {
	return cast.ToFloat64(v)
}

func StringFromAny(v any) string {
	return cast.ToString(v)
}

func BoolFromAny(v any) bool {
	return cast.ToBool(v)
}

func ToInt64Slice(v any) []int64 {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]int64, 0, len(arr))
	for _, item := range arr {
		id := Int64FromAny(item)
		if id > 0 {
			out = append(out, id)
		}
	}
	return out
}

func TimeFromAny(v any) time.Time {
	return cast.ToTime(v).UTC()
}

func ToInt64(s string, fallback int64) int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fallback
	}
	return v
}

// SanitizeFloat returns 0 if v is NaN or Inf, otherwise returns v.
// This prevents JSON serialization errors in the standard library.
func SanitizeFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}
