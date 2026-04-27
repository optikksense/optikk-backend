// Package querycompiler compiles structured log filters into parameterized
// CH WHERE clauses targeting different backends (raw v2, rollup v2, facet
// rollup). Callers pass a Filters struct; Compile(f, Target) returns a
// Compiled{Where, Args, DroppedClauses}. DroppedClauses surfaces to the
// response as `warnings[]` so the UI can explain reduced-fidelity results
// (e.g. body search dropped on the rollup path).
package querycompiler

// Target selects which backend table shape the compiler targets. Each target
// honours a different subset of Filters; unsupported fields are dropped.
type Target int

const (
	// TargetRaw compiles for observability.logs_v2 (full fidelity).
	TargetRaw Target = iota
	// TargetRollup compiles for logs_rollup_{1m,5m,1h}. Only rollup-key
	// dims (severity_bucket, service, environment, host, pod) survive.
	TargetRollup
	// TargetFacetRollup compiles for logs_facets_rollup_5m. Same key set as
	// TargetRollup; sumState / uniqHLL12State merges happen in caller.
	TargetFacetRollup
)

// AttrFilter is a single attribute predicate on attributes_string[key].
type AttrFilter struct {
	Op    string // "eq" | "neq" | "contains" | "regex"
	Key   string
	Value string
}

// Filters is the fully-parsed request. Structured and DSL parsers both
// produce this shape; compile.go consumes it.
type Filters struct {
	TeamID  int64
	StartMs int64
	EndMs   int64

	Severities   []string
	Services     []string
	Hosts        []string
	Pods         []string
	Containers   []string
	Environments []string
	TraceID      string
	SpanID       string
	Search       string
	SearchMode   string // "ngram" (default) | "exact"

	ExcludeSeverities []string
	ExcludeServices   []string
	ExcludeHosts      []string

	Attributes []AttrFilter
}

// Compiled is the result of compile.go — ready to splat into db.Select.
type Compiled struct {
	PreWhere       string
	Where          string
	Args           []any
	DroppedClauses []string
}
