package analytics

type LogHistogramRowDTO struct {
	TimeBucket string `ch:"time_bucket"`
	Severity   string `ch:"severity"`
	Count      int64  `ch:"count"`
}

type LogVolumeRowDTO struct {
	TimeBucket string `ch:"time_bucket"`
	Total      int64  `ch:"total"`
	Errors     int64  `ch:"errors"`
	Warnings   int64  `ch:"warnings"`
	Infos      int64  `ch:"infos"`
	Debugs     int64  `ch:"debugs"`
	Fatals     int64  `ch:"fatals"`
}

type FacetRowDTO struct {
	Dim   string `ch:"dim"`
	Value string `ch:"value"`
	Count int64  `ch:"count"`
}

type ValueCountRowDTO struct {
	Value string `ch:"value"`
	Count int64  `ch:"count"`
}

type TopGroupRowDTO struct {
	GroupValue string `ch:"grp"`
	SortValue  int64  `ch:"sort_value"`
}

type LogAggregateRowDTO struct {
	TimeBucket string  `ch:"time_bucket"`
	GroupValue string  `ch:"grp"`
	Count      int64   `ch:"cnt"`
	ErrorRate  float64 `ch:"error_rate"`
}

type LogAggregateQuery struct {
	GroupBy    string
	GroupCol   string
	Step       string
	TopN       int
	Metric     string
	FieldLabel string
}
