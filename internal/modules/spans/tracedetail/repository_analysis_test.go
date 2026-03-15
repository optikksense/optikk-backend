package tracedetail

import "testing"

func TestIsRootParentSpanID(t *testing.T) {
	t.Helper()

	cases := map[string]bool{
		"":                     true,
		"0000000000000000":     true,
		"\x00\x00\x00\x00\x00": true,
		"abcd1234ef567890":     false,
	}

	for input, want := range cases {
		if got := isRootParentSpanID(input); got != want {
			t.Fatalf("isRootParentSpanID(%q) = %v, want %v", input, got, want)
		}
	}
}
