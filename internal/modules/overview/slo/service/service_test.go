package service

import (
	"math"
	"testing"
)

func TestRemainingErrorBudgetPercent(t *testing.T) {
	tests := []struct {
		name         string
		availability float64
		expected     float64
	}{
		{
			name:         "perfect availability keeps full budget",
			availability: 100,
			expected:     100,
		},
		{
			name:         "meeting target keeps full budget",
			availability: 99.95,
			expected:     50,
		},
		{
			name:         "breach exhausts budget",
			availability: 99.8,
			expected:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := remainingErrorBudgetPercent(tt.availability); math.Abs(got-tt.expected) > 0.000001 {
				t.Fatalf("expected %.2f, got %.2f", tt.expected, got)
			}
		})
	}
}
