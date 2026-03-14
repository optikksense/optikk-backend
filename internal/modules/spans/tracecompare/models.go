package tracecompare

// ComparisonResult is the response for a trace-vs-trace comparison.
type ComparisonResult struct {
	TraceA         TraceSummary       `json:"traceA"`
	TraceB         TraceSummary       `json:"traceB"`
	MatchedSpans   []SpanPairDiff     `json:"matchedSpans"`
	OnlyInA        []SpanSummary      `json:"onlyInA"`
	OnlyInB        []SpanSummary      `json:"onlyInB"`
	ServiceDeltas  []ServiceDelta     `json:"serviceDeltas"`
	TotalDeltaMs   float64            `json:"totalDeltaMs"`
}

type TraceSummary struct {
	TraceID    string  `json:"traceId"`
	SpanCount  int     `json:"spanCount"`
	DurationMs float64 `json:"durationMs"`
	ErrorCount int     `json:"errorCount"`
	Services   int     `json:"services"`
}

// SpanPairDiff represents a matched span pair across two traces.
type SpanPairDiff struct {
	Signature     SpanSignature `json:"signature"`
	SpanIDA       string        `json:"spanIdA"`
	SpanIDB       string        `json:"spanIdB"`
	DurationMsA   float64       `json:"durationMsA"`
	DurationMsB   float64       `json:"durationMsB"`
	DeltaMs       float64       `json:"deltaMs"`
	DeltaPct      float64       `json:"deltaPct"`
	StatusA       string        `json:"statusA"`
	StatusB       string        `json:"statusB"`
	StatusChanged bool          `json:"statusChanged"`
}

// SpanSignature is used to match spans across two traces.
type SpanSignature struct {
	Service   string `json:"service"`
	Operation string `json:"operation"`
	SpanKind  string `json:"spanKind"`
	Depth     int    `json:"depth"`
}

type SpanSummary struct {
	SpanID     string  `json:"spanId"`
	Service    string  `json:"service"`
	Operation  string  `json:"operation"`
	SpanKind   string  `json:"spanKind"`
	DurationMs float64 `json:"durationMs"`
	Status     string  `json:"status"`
}

// ServiceDelta shows aggregate latency change per service.
type ServiceDelta struct {
	Service      string  `json:"service"`
	TotalMsA     float64 `json:"totalMsA"`
	TotalMsB     float64 `json:"totalMsB"`
	DeltaMs      float64 `json:"deltaMs"`
	SpanCountA   int     `json:"spanCountA"`
	SpanCountB   int     `json:"spanCountB"`
}

// internalSpan is used during comparison computation.
type internalSpan struct {
	SpanID     string
	ParentID   string
	Service    string
	Operation  string
	SpanKind   string
	DurationMs float64
	Status     string
	HasError   bool
	Depth      int
}
