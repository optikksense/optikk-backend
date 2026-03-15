package explorer

// ExploreRequest holds the query parameters for a data exploration query.
type ExploreRequest struct {
	SignalType      string // span, log, metric
	Aggregation     string // count, avg, p95, p99, sum
	GroupBy         string // optional facet field
	FilterService   string // optional service filter
	FilterOperation string // optional operation/name filter
	Mode            string // timeseries, table
}

// ExploreResult is the response returned by the explorer endpoint.
type ExploreResult struct {
	Mode    string           `json:"mode"`
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
}
