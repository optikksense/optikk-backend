// Package querycompiler compiles structured trace filters into parameterized
// CH WHERE clauses for traces_index / spans / spans_rollup / facets.
// Mirrors the logs querycompiler shape. DroppedClauses surfaces to the UI
// as `warnings[]` when a target can't honour every filter (e.g. span-attr
// filter dropped on the traces_index path).
package querycompiler

type Target int

const (
	// TargetTracesIndex compiles for observability.traces_index (per-trace
	// summary rows; fastest list path).
	TargetTracesIndex Target = iota
	// TargetSpansRaw compiles for observability.spans (full fidelity).
	TargetSpansRaw
	// TargetSpansRollup compiles for spans_rollup_{1m,5m,1h}.
	TargetSpansRollup
	// TargetFacetRollup compiles for traces_facets_rollup_5m.
	TargetFacetRollup
)

type AttrFilter struct {
	Op    string
	Key   string
	Value string
}

type Filters struct {
	TeamID  int64
	StartMs int64
	EndMs   int64

	Services     []string
	Operations   []string
	SpanKinds    []string
	HTTPMethods  []string
	HTTPStatuses []string
	Statuses     []string
	Environments []string
	PeerServices []string
	TraceID      string

	MinDurationNs int64
	MaxDurationNs int64

	HasError *bool

	ExcludeServices []string
	ExcludeStatuses []string

	Search     string
	SearchMode string

	Attributes []AttrFilter
}

type Compiled struct {
	PreWhere       string
	Where          string
	Args           []any
	DroppedClauses []string
}
