package topology

// nodeTotalRow is the per-service inbound total (SERVER / CONSUMER) leg.
type nodeTotalRow struct {
	ServiceName  string `ch:"service_name"`
	RequestCount uint64 `ch:"request_count"`
}

// nodeErrorLegRow is the per-service error-only leg; merged with nodeTotalRow
// in service.go to produce nodeAggRow.
type nodeErrorLegRow struct {
	ServiceName string `ch:"service_name"`
	ErrorCount  uint64 `ch:"error_count"`
}

// nodeAggRow is the merged shape the service uses to build ServiceNode. The
// repo no longer scans this directly; it is assembled in service.go.
type nodeAggRow struct {
	ServiceName  string
	RequestCount int64
	ErrorCount   int64
	P50Ms        float64
	P95Ms        float64
	P99Ms        float64
}

// edgeTotalRow is the parent→child call-count leg for the edge build.
type edgeTotalRow struct {
	Source    string `ch:"source"`
	Target    string `ch:"target"`
	CallCount uint64 `ch:"call_count"`
}

// edgeErrorLegRow is the error-only parent→child join, merged into edgeAggRow.
type edgeErrorLegRow struct {
	Source     string `ch:"source"`
	Target     string `ch:"target"`
	ErrorCount uint64 `ch:"error_count"`
}

// edgeAggRow is the merged shape the service uses to build ServiceEdge.
type edgeAggRow struct {
	Source     string
	Target     string
	CallCount  int64
	ErrorCount int64
	P50Ms      float64
	P95Ms      float64
}
