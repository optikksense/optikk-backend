package exprparser

import (
	"testing"
)

func TestEvaluate(t *testing.T) {
	tests := []struct {
		expr     string
		resolve  func(string) float64
		expected float64
		wantErr  bool
	}{
		{
			expr:     "1 + 2 * 3",
			resolve:  func(s string) float64 { return 0 },
			expected: 7,
			wantErr:  false,
		},
		{
			expr:     "(1 + 2) * 3",
			resolve:  func(s string) float64 { return 0 },
			expected: 9,
			wantErr:  false,
		},
		{
			expr:     "10 / 2 - 1",
			resolve:  func(s string) float64 { return 0 },
			expected: 4,
			wantErr:  false,
		},
		{
			expr: "a + b",
			resolve: func(s string) float64 {
				if s == "a" {
					return 10
				}
				if s == "b" {
					return 5
				}
				return 0
			},
			expected: 15,
			wantErr:  false,
		},
		{
			expr: "a * (b + 2)",
			resolve: func(s string) float64 {
				if s == "a" {
					return 2
				}
				if s == "b" {
					return 3
				}
				return 0
			},
			expected: 10,
			wantErr:  false,
		},
		{
			expr:     "10 / 0",
			resolve:  func(s string) float64 { return 0 },
			expected: 0,
			wantErr:  false,
		},
		{
			expr:    "1 + unknown",
			resolve: func(s string) float64 { return 0 },
			wantErr: true, // "unknown" is too long for a single-letter variable
		},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			got, err := Evaluate(tt.expr, tt.resolve)
			if (err != nil) != tt.wantErr {
				t.Errorf("Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.expected {
				t.Errorf("Evaluate() got = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestExtractVariables(t *testing.T) {
	tests := []struct {
		expr     string
		expected []string
	}{
		{
			expr:     "a + b * c",
			expected: []string{"a", "b", "c"},
		},
		{
			expr:     "a + a + b",
			expected: []string{"a", "b"},
		},
		{
			expr:     "1 + 2 * (3 / 4)",
			expected: nil,
		},
		{
			expr:     "x * (y - z) / x",
			expected: []string{"x", "y", "z"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			got := ExtractVariables(tt.expr)
			if len(got) != len(tt.expected) {
				t.Errorf("ExtractVariables() got = %v, want %v", got, tt.expected)
				return
			}
			for i, v := range got {
				if v != tt.expected[i] {
					t.Errorf("ExtractVariables() got[%d] = %v, want[%d] = %v", i, v, i, tt.expected[i])
				}
			}
		})
	}
}
