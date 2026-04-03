package metricsexplorer

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

// tagKeyDTO scans the result of ListTagKeys.
type tagKeyDTO struct {
	TagKey string `ch:"tag_key"`
}

// tagValueDTO scans the result of ListTagValues.
type tagValueDTO struct {
	TagValue string `ch:"tag_value"`
	Count    int64  `ch:"count"`
}

// timeseriesPointDTO scans a single row from QueryTimeseries.
type timeseriesPointDTO struct {
	Timestamp string  `ch:"time_bucket"`
	Value     float64 `ch:"agg_value"`
}
