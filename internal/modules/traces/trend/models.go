package trend

// TrendBucket is one display-grain bucket of total + error counts returned by
// POST /traces/trend.
type TrendBucket struct {
	TimeBucket string `json:"time_bucket"`
	Total      uint64 `json:"total"`
	Errors     uint64 `json:"errors"`
}
