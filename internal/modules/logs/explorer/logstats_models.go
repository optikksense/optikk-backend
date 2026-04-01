package explorer

// JSON response types for log stats / histogram / volume / aggregate APIs (formerly logs/analytics).

type LogHistogramBucket struct {
	TimeBucket string `json:"time_bucket"`
	Severity   string `json:"severity"`
	Count      int64  `json:"count"`
}

type LogHistogramData struct {
	Buckets []LogHistogramBucket `json:"buckets"`
	Step    string               `json:"step"`
}

type LogVolumeBucket struct {
	TimeBucket string `json:"time_bucket"`
	Total      int64  `json:"total"`
	Errors     int64  `json:"errors"`
	Warnings   int64  `json:"warnings"`
	Infos      int64  `json:"infos"`
	Debugs     int64  `json:"debugs"`
	Fatals     int64  `json:"fatals"`
}

type LogVolumeData struct {
	Buckets []LogVolumeBucket `json:"buckets"`
	Step    string            `json:"step"`
}

type LogStats struct {
	Total  int64              `json:"total"`
	Fields map[string][]Facet `json:"fields"`
}

type Facet struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

type FieldValuesResponse struct {
	Field  string  `json:"field"`
	Values []Facet `json:"values"`
}

type LogAggregateRequest struct {
	GroupBy string `json:"group_by" form:"group_by"`
	Step    string `json:"step" form:"step"`
	TopN    int    `json:"top_n" form:"top_n"`
	Metric  string `json:"metric" form:"metric"`
}

type LogAggregateRow struct {
	TimeBucket string  `json:"time_bucket"`
	GroupValue string  `json:"group_value"`
	Count      int64   `json:"count"`
	ErrorRate  float64 `json:"error_rate,omitempty"`
}

type LogAggregateResponse struct {
	GroupBy string            `json:"group_by"`
	Step    string            `json:"step"`
	Metric  string            `json:"metric"`
	Rows    []LogAggregateRow `json:"rows"`
}
