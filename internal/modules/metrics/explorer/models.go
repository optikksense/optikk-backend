package explorer

// ---------------------------------------------------------------------------
// Internal domain models (returned by repository methods).
//
// Filter / query input shapes live in internal/modules/metrics/filter — the
// repo consumes filter.Filters directly. The types below are output-only.
// ---------------------------------------------------------------------------

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
	Count    uint64 `json:"count"     ch:"count"`
}

// TimeseriesPoint is a single aggregated data point from the explorer query.
type TimeseriesPoint struct {
	Timestamp string  `json:"timestamp" ch:"time_bucket"`
	Value     float64 `json:"value"     ch:"agg_value"`
}

// ---------------------------------------------------------------------------
// Frontend-facing models (match the optic-frontend Zod schemas)
// ---------------------------------------------------------------------------

// FEMetricNameEntry matches the frontend metricNameEntrySchema.
type FEMetricNameEntry struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Unit        string `json:"unit,omitempty"`
	Description string `json:"description,omitempty"`
}

// FEMetricNamesResponse matches the frontend metricNamesResponseSchema.
type FEMetricNamesResponse struct {
	Metrics []FEMetricNameEntry `json:"metrics"`
}

// FETagEntry matches the frontend metricTagSchema.
type FETagEntry struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

// FETagsResponse matches the frontend metricTagsResponseSchema.
type FETagsResponse struct {
	Tags []FETagEntry `json:"tags"`
}

// FEFilter matches the frontend filter shape in query requests.
type FEFilter struct {
	Key      string `json:"key"`
	Operator string `json:"operator"` // "eq", "neq", "in", "not_in"
	Value    any    `json:"value"`    // string or []string
}

// FEMetricQuery matches the frontend query shape in query requests.
type FEMetricQuery struct {
	ID               string     `json:"id"`
	Aggregation      string     `json:"aggregation"`
	MetricName       string     `json:"metricName"`
	Where            []FEFilter `json:"where"`
	GroupBy          []string   `json:"groupBy,omitempty"`
	SpaceAggregation string     `json:"spaceAggregation,omitempty"`
}

// FEQueryRequest matches the frontend MetricExplorerQueryRequest.
type FEQueryRequest struct {
	StartTime int64           `json:"startTime"`
	EndTime   int64           `json:"endTime"`
	Step      string          `json:"step"`
	Queries   []FEMetricQuery `json:"queries"`
}

// FESeries matches the frontend metricSeriesSchema.
type FESeries struct {
	Tags   map[string]string `json:"tags"`
	Values []*float64        `json:"values"` // nullable for missing points
}

// FEQueryResult matches the frontend metricQueryResultSchema.
type FEQueryResult struct {
	Timestamps []int64    `json:"timestamps"`
	Series     []FESeries `json:"series"`
}

// FEQueryResponse matches the frontend metricsExplorerResponseSchema.
type FEQueryResponse struct {
	Results map[string]FEQueryResult `json:"results"`
}
