package explorer

// ---------------------------------------------------------------------------
// Metric Names
// ---------------------------------------------------------------------------

// MetricNameEntry represents a single metric with its metadata.
type MetricNameEntry struct {
	Name        string `json:"name"        ch:"name"`
	Type        string `json:"type"        ch:"type"`
	Unit        string `json:"unit"        ch:"unit"`
	Description string `json:"description" ch:"description"`
}

// MetricNamesResponse is the response for the metric names endpoint.
type MetricNamesResponse struct {
	Metrics []MetricNameEntry `json:"metrics"`
}

// ---------------------------------------------------------------------------
// Metric Tags
// ---------------------------------------------------------------------------

// MetricTag represents a tag key with its observed values.
type MetricTag struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

// MetricTagsResponse is the response for the metric tags endpoint.
type MetricTagsResponse struct {
	Tags []MetricTag `json:"tags"`
}

// tagKeyDTO is the ClickHouse row for distinct tag keys.
type tagKeyDTO struct {
	TagKey string `ch:"tag_key"`
}

// tagValueDTO is the ClickHouse row for distinct tag values.
type tagValueDTO struct {
	TagValue string `ch:"tag_value"`
}

// tagKeyValuesDTO is the ClickHouse row for batched tag key + values discovery.
type tagKeyValuesDTO struct {
	TagKey    string   `ch:"tag_key"`
	TagValues []string `ch:"tag_values"`
}

// ---------------------------------------------------------------------------
// Metric Explorer Query
// ---------------------------------------------------------------------------

// MetricExplorerRequest is the request body for the metric query endpoint.
type MetricExplorerRequest struct {
	StartTime int64                `json:"startTime"`
	EndTime   int64                `json:"endTime"`
	Step      string               `json:"step"`
	Queries   []MetricQueryRequest `json:"queries"`
}

// MetricQueryRequest represents a single query within the explorer request.
type MetricQueryRequest struct {
	ID               string              `json:"id"`
	Aggregation      string              `json:"aggregation"`
	MetricName       string              `json:"metricName"`
	Where            []MetricWhereClause `json:"where"`
	GroupBy          []string            `json:"groupBy"`
	SpaceAggregation string              `json:"spaceAggregation,omitempty"`
}

// MetricWhereClause represents a single filter condition.
type MetricWhereClause struct {
	Key      string `json:"key"`
	Operator string `json:"operator"` // eq, neq, in, not_in, wildcard
	Value    any    `json:"value"`    // string or []string
}

// MetricSeriesData represents a single time series with tag values.
type MetricSeriesData struct {
	Tags   map[string]string `json:"tags"`
	Values []*float64        `json:"values"`
}

// MetricQueryResult holds timestamps and series for a single query.
type MetricQueryResult struct {
	Timestamps []int64            `json:"timestamps"`
	Series     []MetricSeriesData `json:"series"`
}

// MetricExplorerResponse is the top-level response for the query endpoint.
type MetricExplorerResponse struct {
	Results map[string]*MetricQueryResult `json:"results"`
}

// ---------------------------------------------------------------------------
// Internal DTOs for ClickHouse scanning
// ---------------------------------------------------------------------------

// timeseriesRowDTO is used to scan grouped timeseries results from ClickHouse.
type timeseriesRowDTO struct {
	Ts       int64    `ch:"ts"`
	TagKeys  []string `ch:"tag_keys"`
	TagVals  []string `ch:"tag_vals"`
	AggValue float64  `ch:"agg_value"`
}
