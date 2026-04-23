// Package trace_shape exposes the "shape/distribution of a trace" endpoints
// (flamegraph, span-kind breakdown) split out of the legacy tracedetail module.
// Self-times are computed on the frontend from the span list returned by
// /traces/:id/bundle — no server-side endpoint.
package trace_shape //nolint:revive,stylecheck

type SpanKindDuration struct {
	SpanKind    string  `json:"span_kind"         ch:"span_kind"`
	TotalDuraMs float64 `json:"total_duration_ms" ch:"total_duration_ms"`
	SpanCount   int64   `json:"span_count"        ch:"span_count"`
	PctOfTrace  float64 `json:"pct_of_trace"`
}

// FlamegraphFrame represents a single frame in a flamegraph visualization.
// Frames are ordered depth-first with self-time computed.
type FlamegraphFrame struct {
	SpanID     string  `json:"span_id"`
	Name       string  `json:"name"` // "service :: operation"
	Service    string  `json:"service"`
	Operation  string  `json:"operation"`
	DurationMs float64 `json:"duration_ms"`
	SelfTimeMs float64 `json:"self_time_ms"`
	Level      int     `json:"level"`
	SpanKind   string  `json:"span_kind"`
	HasError   bool    `json:"has_error"`
}
