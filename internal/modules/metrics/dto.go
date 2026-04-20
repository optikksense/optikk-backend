package metrics

// ClickHouse scan DTOs — these match the column aliases in repository queries.
// They are intentionally separate from the API-facing models so the DB shape
// can evolve independently of the JSON contract.

// metricNameDTO scans the result of ListMetricNames.
type metricNameDTO struct {
	MetricName  string `ch:"metric_name"`
	MetricType  string `ch:"metric_type"`
	Unit        string `ch:"unit"`
	Description string `ch:"description"`
}

// tagKeyDTO scans the result of ListTagKeys (legacy aggregation path — kept
// so any other reader can still use the shape).
type tagKeyDTO struct {
	TagKey string `ch:"tag_key"`
}

// attributesRow is the scan target for the new ListTagKeys sampling path:
// grab the attributes map for N recent rows, collect distinct keys in Go.
// LIMIT on the sample is set in repository.go; 2000 matches the Datadog /
// Prometheus tag-discovery default and trades <1-in-2000 miss rate for a
// near-constant-time query.
type attributesRow struct {
	Attributes map[string]string `ch:"attributes"`
}

// tagValueDTO scans the result of ListTagValues.
type tagValueDTO struct {
	TagValue string `ch:"tag_value"`
	Count    uint64 `ch:"count"`
}

// timeseriesPointDTO scans a single row from QueryTimeseries.
type timeseriesPointDTO struct {
	Timestamp string  `ch:"time_bucket"`
	Value     float64 `ch:"agg_value"`
}
