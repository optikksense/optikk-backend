// Package trace_analytics exposes POST /api/v1/traces/analytics — group-by +
// aggregation queries over traces_index. Split out of the legacy explorer
// module so the core list + single-trace endpoints stay focused.
package trace_analytics //nolint:revive,stylecheck

type Aggregation struct {
	Fn    string `json:"fn"`
	Field string `json:"field"`
	Alias string `json:"alias"`
}

// AnalyticsRow is the untyped column map produced by the group-by query; the
// frontend picks columns dynamically so a generic shape is the right fit.
type AnalyticsRow map[string]any

type AnalyticsResponse struct {
	VizMode  string         `json:"vizMode"`
	Step     string         `json:"step,omitempty"`
	Rows     []AnalyticsRow `json:"rows"`
	Warnings []string       `json:"warnings,omitempty"`
}
