package explorer

// ClickHouse scan DTOs for log stats repository.
//
// Counts scan as uint64 (CH's native count() return type) and are cast in
// the service layer. Keeping the SQL free of toInt64/toUInt32 keeps queries
// database-native — every conditional / type-coerce happens in Go.

type logHistogramRowDTO struct {
	TimeBucket string `ch:"time_bucket"`
	Severity   string `ch:"severity"`
	Count      uint64 `ch:"count"`
}

// logVolumeRawRow is the CH scan target for GetLogVolume. Service layer
// pivots [(bucket, severity, count)] into the pivoted logVolumeRowDTO that
// callers expect (total / errors / warnings / infos / debugs / fatals per
// bucket). Queries stay pure GROUP BY — no if/multiIf/sumIf.
type logVolumeRawRow struct {
	TimeBucket string `ch:"time_bucket"`
	Severity   string `ch:"severity_text"`
	Count      uint64 `ch:"count"`
}

type logVolumeRowDTO struct {
	TimeBucket string `ch:"time_bucket"`
	Total      int64
	Errors     int64
	Warnings   int64
	Infos      int64
	Debugs     int64
	Fatals     int64
}

type facetRowDTO struct {
	Dim   string `ch:"dim"`
	Value string `ch:"value"`
	Count int64  `ch:"count"`
}

// facetScanRow is the per-facet scan shape used inside GetLogStats; the
// outer dim label is attached in Go.
type facetScanRow struct {
	Value string `ch:"value"`
	Count uint64 `ch:"count"`
}

type valueCountRowDTO struct {
	Value string `ch:"value"`
	Count uint64 `ch:"count"`
}

type topGroupRowDTO struct {
	GroupValue string `ch:"grp"`
	SortValue  uint64 `ch:"sort_value"`
}

// logAggregateScanRow is what GetAggregateSeries fetches from CH: the pure
// per-(bucket, grp) count. Error rate is computed in Go from a second
// narrower-scan query (severity IN ('ERROR','FATAL')) in the service layer.
type logAggregateScanRow struct {
	TimeBucket string `ch:"time_bucket"`
	GroupValue string `ch:"grp"`
	Count      uint64 `ch:"count"`
}

type logAggregateRowDTO struct {
	TimeBucket string
	GroupValue string
	Count      int64
	ErrorRate  float64
}

type logAggregateQuery struct {
	GroupBy  string
	GroupCol string
	Step     string
	TopN     int
	Metric   string
}
