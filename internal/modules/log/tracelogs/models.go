package tracelogs

import shared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"

type TraceLogsResponse struct {
	Logs          []shared.Log `json:"logs"`
	IsSpeculative bool         `json:"is_speculative"`
}
