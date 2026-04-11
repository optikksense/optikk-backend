package hub

import "time"

// --- Scores ---

type CreateScoreRequest struct {
	Name            string   `json:"name"`
	Value           float64  `json:"value"`
	TraceID         string   `json:"trace_id"`
	SpanID          string   `json:"span_id"`
	SessionID       string   `json:"session_id"`
	PromptTemplate  string   `json:"prompt_template"`
	Model           string   `json:"model"`
	Source          string   `json:"source"`
	Rationale       string   `json:"rationale"`
}

type BatchCreateScoresRequest struct {
	Scores []CreateScoreRequest `json:"scores"`
}

type ScoreDTO struct {
	ID             int64     `json:"id"`
	Name           string    `json:"name"`
	Value          float64   `json:"value"`
	TraceID        string    `json:"trace_id"`
	SpanID         string    `json:"span_id"`
	SessionID      string    `json:"session_id,omitempty"`
	PromptTemplate string    `json:"prompt_template,omitempty"`
	Model          string    `json:"model,omitempty"`
	Source         string    `json:"source"`
	Rationale      string    `json:"rationale,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type ListScoresResponse struct {
	Results  []ScoreDTO `json:"results"`
	PageInfo PageInfo   `json:"pageInfo"`
}

type PageInfo struct {
	Total  int64 `json:"total"`
	Offset int   `json:"offset"`
	Limit  int   `json:"limit"`
}

// --- Prompts ---

type CreatePromptRequest struct {
	Slug        string `json:"slug"`
	DisplayName string `json:"display_name"`
	Body        string `json:"body"`
}

type UpdatePromptRequest struct {
	DisplayName *string `json:"display_name"`
	Body        *string `json:"body"`
}

type PromptDTO struct {
	ID          int64     `json:"id"`
	Slug        string    `json:"slug"`
	DisplayName string    `json:"display_name"`
	Body        string    `json:"body"`
	Version     int       `json:"version"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   *time.Time `json:"updated_at,omitempty"`
}

type ListPromptsResponse struct {
	Results []PromptDTO `json:"results"`
}

// --- Datasets ---

type CreateDatasetRequest struct {
	Name          string           `json:"name"`
	QuerySnapshot string           `json:"query_snapshot"`
	StartTimeMs   int64            `json:"start_time_ms"`
	EndTimeMs     int64            `json:"end_time_ms"`
	Generations   []map[string]any `json:"generations"`
}

type DatasetDTO struct {
	ID            int64     `json:"id"`
	Name          string    `json:"name"`
	QuerySnapshot string    `json:"query_snapshot,omitempty"`
	StartTimeMs   int64     `json:"start_time_ms"`
	EndTimeMs     int64     `json:"end_time_ms"`
	RowCount      int       `json:"row_count"`
	CreatedAt     time.Time `json:"created_at"`
}

type DatasetDetailDTO struct {
	DatasetDTO
	PayloadJSON string `json:"payload_json"`
}

type ListDatasetsResponse struct {
	Results []DatasetDTO `json:"results"`
}

// --- Team settings ---

type TeamSettingsDTO struct {
	PricingOverrides map[string]PricingOverride `json:"pricing_overrides"`
	UpdatedAt        *time.Time                 `json:"updated_at,omitempty"`
}

type PricingOverride struct {
	InputPer1K  float64 `json:"inputPer1K"`
	OutputPer1K float64 `json:"outputPer1K"`
}

type PatchTeamSettingsRequest struct {
	PricingOverrides map[string]PricingOverride `json:"pricing_overrides"`
}
