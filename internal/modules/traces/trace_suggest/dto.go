package trace_suggest //nolint:revive,stylecheck

// SuggestRequest is the wire payload for POST /api/v1/traces/suggest.
type SuggestRequest struct {
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
	Field     string `json:"field"`
	Prefix    string `json:"prefix"`
	Limit     int    `json:"limit"`
}

// suggestionRow is the scan target for both the traces_index and spans paths.
type suggestionRow struct {
	Value string `ch:"value"`
	Count uint64 `ch:"count"`
}
