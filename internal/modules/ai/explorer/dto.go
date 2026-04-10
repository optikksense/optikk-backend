package explorer

import "time"

// -------- API response DTOs --------

type SpanDTO struct {
	SpanID        string    `json:"spanId"`
	TraceID       string    `json:"traceId"`
	ParentSpanID  string    `json:"parentSpanId"`
	ServiceName   string    `json:"serviceName"`
	OperationName string    `json:"operationName"`
	Model         string    `json:"model"`
	Provider      string    `json:"provider"`
	OperationType string    `json:"operationType"`
	Timestamp     time.Time `json:"timestamp"`
	DurationMs    float64   `json:"durationMs"`
	InputTokens   int64     `json:"inputTokens"`
	OutputTokens  int64     `json:"outputTokens"`
	TotalTokens   int64     `json:"totalTokens"`
	HasError      bool      `json:"hasError"`
	StatusMessage string    `json:"statusMessage"`
	FinishReason  string    `json:"finishReason"`
	Temperature   float64   `json:"temperature"`
}

type FacetDTO struct {
	Values []FacetValueDTO `json:"values"`
}

type FacetValueDTO struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

type FacetsResponseDTO struct {
	Models        FacetDTO `json:"models"`
	Providers     FacetDTO `json:"providers"`
	Operations    FacetDTO `json:"operations"`
	Services      FacetDTO `json:"services"`
	FinishReasons FacetDTO `json:"finishReasons"`
}

type ExplorerSummaryDTO struct {
	TotalSpans   int64   `json:"totalSpans"`
	ErrorCount   int64   `json:"errorCount"`
	AvgLatencyMs float64 `json:"avgLatencyMs"`
	P95Ms        float64 `json:"p95Ms"`
	TotalTokens  int64   `json:"totalTokens"`
	UniqueModels int64   `json:"uniqueModels"`
}

type HistogramPointDTO struct {
	Timestamp time.Time `json:"timestamp"`
	Count     int64     `json:"count"`
}

// -------- Filter params --------

type ExplorerFilter struct {
	TeamID       int64
	StartMs      int64
	EndMs        int64
	Service      string
	Model        string
	Provider     string
	Operation    string
	Status       string // "error" or "ok"
	FinishReason string
	MinDurationMs float64
	MaxDurationMs float64
	TraceID      string
	Limit        int
	Offset       int
	Sort         string // "timestamp", "duration", "tokens"
	SortDir      string // "asc", "desc"
}
