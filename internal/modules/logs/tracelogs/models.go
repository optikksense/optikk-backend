package tracelogs

import shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"

type TraceLogsResponse struct {
	Logs          []shared.Log `json:"logs"`
	IsSpeculative bool         `json:"is_speculative"`
}
