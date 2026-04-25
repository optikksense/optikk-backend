// Package trace_suggest backs POST /api/v1/traces/suggest — live field/value
// autocomplete for the Datadog-style query DSL bar. Split out of explorer so
// the suggestions query path stays isolated from the main list query.
package trace_suggest //nolint:revive,stylecheck

type Suggestion struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
}

type SuggestResponse struct {
	Suggestions []Suggestion `json:"suggestions"`
}
