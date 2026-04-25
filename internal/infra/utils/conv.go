package utils

import (
	"math"
	"strconv"
)

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
