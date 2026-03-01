package traces

import "testing"

func TestRawTimeBucketExprUsesSpanTimestamps(t *testing.T) {
	tests := []struct {
		name     string
		startMs  int64
		endMs    int64
		expected string
	}{
		{
			name:     "short range",
			startMs:  0,
			endMs:    3_600_000,
			expected: "toStartOfMinute(start_time)",
		},
		{
			name:     "day range",
			startMs:  0,
			endMs:    24 * 3_600_000,
			expected: "toStartOfFiveMinutes(start_time)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rawTimeBucketExpr(tt.startMs, tt.endMs, "start_time"); got != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}
