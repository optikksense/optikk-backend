package errors

type errorGroupRow struct {
	ExceptionType string `ch:"exception_type"`
	StatusMessage string `ch:"status_message"`
	Service       string `ch:"service"`
	Count         uint64 `ch:"count"`
}

type timeseriesRow struct {
	TimeBucket string `ch:"time_bucket"`
	Errors     uint64 `ch:"errors"`
	Total      uint64 `ch:"total"`
}
