package explorer

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
	Count    uint64 `ch:"count"`
}

// tagKeyValueDTO scans the result of ListTagValuesForKeys — one (key, value)
// pair per row, so every tag key's values come back in a single query.
type tagKeyValueDTO struct {
	TagKey   string `ch:"tag_key"`
	TagValue string `ch:"tag_value"`
	Count    uint64 `ch:"count"`
}

// timeseriesPointDTO scans a single row from QueryTimeseries.
type timeseriesPointDTO struct {
	TsBucket uint32  `ch:"ts_bucket"`
	Sum      float64 `ch:"val_sum"`
	Count    uint64  `ch:"val_count"`
	Min      float64 `ch:"val_min"`
	Max      float64 `ch:"val_max"`
}
