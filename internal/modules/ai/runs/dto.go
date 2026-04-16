package runs

import (
	aishared "github.com/Optikk-Org/optikk-backend/internal/modules/ai/shared"
)

// QueryRequest is the POST body for the AI runs search endpoint.
type QueryRequest struct {
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
	Query     string `json:"query"`
	Limit     int    `json:"limit"`
	Offset    int    `json:"offset"`
	Step      string `json:"step"`
	OrderBy   string `json:"orderBy,omitempty"`
	OrderDir  string `json:"orderDir,omitempty"`
}

// AIPageInfo carries pagination metadata.
type AIPageInfo struct {
	Total   uint64 `json:"total"`
	Offset  int    `json:"offset"`
	Limit   int    `json:"limit"`
	HasMore bool   `json:"hasMore"`
}

// AIExplorerFacets holds faceted search results for the query endpoint.
type AIExplorerFacets struct {
	Provider       []aishared.FacetBucket `json:"provider"`
	RequestModel   []aishared.FacetBucket `json:"request_model"`
	Operation      []aishared.FacetBucket `json:"operation"`
	ServiceName    []aishared.FacetBucket `json:"service_name"`
	PromptTemplate []aishared.FacetBucket `json:"prompt_template"`
	FinishReason   []aishared.FacetBucket `json:"finish_reason"`
	GuardrailState []aishared.FacetBucket `json:"guardrail_state"`
	QualityBucket  []aishared.FacetBucket `json:"quality_bucket"`
	Status         []aishared.FacetBucket `json:"status"`
}

// AICorrelations are extra facet slices attached to the query response.
type AICorrelations struct {
	TopModels  []aishared.FacetBucket `json:"top_models,omitempty"`
	TopPrompts []aishared.FacetBucket `json:"top_prompts,omitempty"`
}

// AIExplorerResponse is the combined response for the runs query endpoint.
type AIExplorerResponse struct {
	Results      []aishared.AIRunRow  `json:"results"`
	Summary      aishared.AIOverview  `json:"summary"`
	Facets       AIExplorerFacets     `json:"facets"`
	Trend        []aishared.AITrendPoint `json:"trend"`
	PageInfo     AIPageInfo           `json:"pageInfo"`
	Correlations AICorrelations       `json:"correlations,omitempty"`
}

// AIRelatedLog is one log line correlated with a run.
type AIRelatedLog struct {
	Timestamp    string `json:"timestamp"`
	SeverityText string `json:"severity_text"`
	Body         string `json:"body"`
	ServiceName  string `json:"service_name"`
	TraceID      string `json:"trace_id"`
	SpanID       string `json:"span_id"`
}

// AIAlertTargetRef is a matching alert rule for a run.
type AIAlertTargetRef struct {
	AlertID       int64          `json:"alert_id"`
	RuleName      string         `json:"rule_name"`
	ConditionType string         `json:"condition_type"`
	RuleState     string         `json:"rule_state"`
	TargetRef     map[string]any `json:"target_ref,omitempty"`
}

// AIRunRelated bundles all correlated telemetry for a single run.
type AIRunRelated struct {
	RunID          string             `json:"run_id"`
	TraceID        string             `json:"trace_id"`
	SpanID         string             `json:"span_id"`
	ServiceName    string             `json:"service_name"`
	ServiceVersion string             `json:"service_version"`
	Environment    string             `json:"environment"`
	Logs           []AIRelatedLog     `json:"logs"`
	Alerts         []AIAlertTargetRef `json:"alerts"`
}
