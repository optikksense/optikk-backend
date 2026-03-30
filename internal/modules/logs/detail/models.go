package detail

import shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"

type LogSurroundingResponse struct {
	Anchor shared.Log   `json:"anchor"`
	Before []shared.Log `json:"before"`
	After  []shared.Log `json:"after"`
}

type LogDetailResponse struct {
	Log         shared.Log   `json:"log"`
	ContextLogs []shared.Log `json:"context_logs"`
}
