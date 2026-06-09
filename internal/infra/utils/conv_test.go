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

func TestSanitizeFloatPtr(t *testing.T) {
	if SanitizeFloatPtr(nil) != nil {
		t.Error("SanitizeFloatPtr(nil) should be nil")
	}
	nan := math.NaN()
	if SanitizeFloatPtr(&nan) != nil {
		t.Error("SanitizeFloatPtr(NaN) should be nil")
	}
	v := 2.5
	if got := SanitizeFloatPtr(&v); got == nil || *got != 2.5 {
		t.Errorf("SanitizeFloatPtr(2.5) = %v, want 2.5", got)
	}
}
