package metricsexplorer

// MetricNameResult holds a metric name for autocomplete search.
type MetricNameResult struct {
	MetricName  string `json:"metric_name"  ch:"metric_name"`
	MetricType  string `json:"metric_type"  ch:"metric_type"`
	Unit        string `json:"unit"         ch:"unit"`
	Description string `json:"description"  ch:"description"`
}

// TagKeyResult holds an attribute key available for a given metric.
type TagKeyResult struct {
	TagKey string `json:"tag_key" ch:"tag_key"`
}

// TagValueResult holds an attribute value and its occurrence count.
type TagValueResult struct {
	TagValue string `json:"tag_value" ch:"tag_value"`
	Count    int64  `json:"count"     ch:"count"`
}

// TimeseriesPoint is a single aggregated data point from the explorer query.
type TimeseriesPoint struct {
	Timestamp string  `json:"timestamp" ch:"time_bucket"`
	Value     float64 `json:"value"     ch:"agg_value"`
}

// TagFilter is a single dimension filter clause in a query.
type TagFilter struct {
	Key      string   `json:"key"`
	Operator string   `json:"operator"` // "=", "!=", "IN", "NOT IN"
	Values   []string `json:"values"`
}

// MetricQuery defines a single metric query line in the explorer.
type MetricQuery struct {
	Name        string      `json:"name"`              // query label: "a", "b", etc.
	MetricName  string      `json:"metricName"`        // e.g. "http.server.request.duration"
	Aggregation string      `json:"aggregation"`       // avg, sum, min, max, count, p50, p75, p95, p99
	GroupBy     []string    `json:"groupBy,omitempty"` // tag keys to group by
	Filters     []TagFilter `json:"filters,omitempty"` // dimension filters
}

// Formula combines query results with arithmetic.
type Formula struct {
	Name       string `json:"name"`       // label for the formula series
	Expression string `json:"expression"` // e.g. "a / b", "a + b * 100"
}

// ExplorerQueryRequest is the POST body for the main query endpoint.
type ExplorerQueryRequest struct {
	Queries  []MetricQuery `json:"queries"`
	Formulas []Formula     `json:"formulas,omitempty"`
}

// Datapoint is a timestamp-value pair in a time series.
type Datapoint struct {
	Timestamp string  `json:"timestamp"`
	Value     float64 `json:"value"`
}

// SeriesResult is one named series in the query response.
type SeriesResult struct {
	Name       string            `json:"name"`
	GroupTags  map[string]string `json:"groupTags"`
	Datapoints []Datapoint       `json:"datapoints"`
}

// ExplorerQueryResult is the response from the query endpoint.
type ExplorerQueryResult struct {
	Series []SeriesResult `json:"series"`
}
