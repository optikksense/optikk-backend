package utils

import (
	"math"
	"testing"
)

func TestSanitizeFloat(t *testing.T) {
	cases := []struct {
		in   float64
		want float64
	}{
		{1.5, 1.5},
		{0, 0},
		{math.NaN(), 0},
		{math.Inf(1), 0},
		{math.Inf(-1), 0},
	}
	for _, c := range cases {
		if got := SanitizeFloat(c.in); got != c.want {
			t.Errorf("SanitizeFloat(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

