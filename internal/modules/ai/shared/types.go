package shared

import (
	"regexp"
	"strings"
	"time"
)

const (
	DefaultPageSize       = 25
	MaxPageSize           = 200
	DefaultBreakdownLimit = 12
	DefaultLogLimit       = 50
	MaxSnippetLength      = 900
)

// QueryContext carries the per-request parameters needed by every AI query.
type QueryContext struct {
	TeamID int64
	Start  int64
	End    int64
	Where  string
	Args   []any
}

// FacetBucket is a single value+count pair used in faceted search results.
type FacetBucket struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
}

// AIOverview is the aggregate summary returned by the overview and query endpoints.
type AIOverview struct {
	TotalRuns        uint64  `json:"total_runs" ch:"total_runs"`
	ErrorRuns        uint64  `json:"error_runs" ch:"error_runs"`
	ErrorRatePct     float64 `json:"error_rate_pct"`
	AvgLatencyMs     float64 `json:"avg_latency_ms" ch:"avg_latency_ms"`
	P95LatencyMs     float64 `json:"p95_latency_ms" ch:"p95_latency_ms"`
	AvgTTFTMs        float64 `json:"avg_ttft_ms" ch:"avg_ttft_ms"`
	TotalTokens      float64 `json:"total_tokens" ch:"total_tokens"`
	TotalCostUSD     float64 `json:"total_cost_usd" ch:"total_cost_usd"`
	AvgQualityScore  float64 `json:"avg_quality_score" ch:"avg_quality_score"`
	ProviderCount    uint64  `json:"provider_count" ch:"provider_count"`
	ModelCount       uint64  `json:"model_count" ch:"model_count"`
	PromptCount      uint64  `json:"prompt_count" ch:"prompt_count"`
	GuardrailBlocked uint64  `json:"guardrail_blocked" ch:"guardrail_blocked"`
}

// AITrendPoint is one time-series bucket for overview and query trend charts.
type AITrendPoint struct {
	TimeBucket       time.Time `json:"time_bucket" ch:"time_bucket"`
	Requests         uint64    `json:"requests" ch:"requests"`
	ErrorRuns        uint64    `json:"error_runs" ch:"error_runs"`
	ErrorRatePct     float64   `json:"error_rate_pct"`
	P95LatencyMs     float64   `json:"p95_latency_ms" ch:"p95_latency_ms"`
	AvgTTFTMs        float64   `json:"avg_ttft_ms" ch:"avg_ttft_ms"`
	TotalTokens      float64   `json:"total_tokens" ch:"total_tokens"`
	TotalCostUSD     float64   `json:"total_cost_usd" ch:"total_cost_usd"`
	AvgQualityScore  float64   `json:"avg_quality_score" ch:"avg_quality_score"`
	GuardrailBlocked uint64    `json:"guardrail_blocked" ch:"guardrail_blocked"`
}

// AIRunRow is the per-run row returned by search and explorer queries.
type AIRunRow struct {
	RunID                 string    `json:"run_id" ch:"run_id"`
	TraceID               string    `json:"trace_id" ch:"trace_id"`
	SpanID                string    `json:"span_id" ch:"span_id"`
	ServiceName           string    `json:"service_name" ch:"service_name"`
	Provider              string    `json:"provider" ch:"provider"`
	RequestModel          string    `json:"request_model" ch:"request_model"`
	ResponseModel         string    `json:"response_model" ch:"response_model"`
	Operation             string    `json:"operation" ch:"operation"`
	PromptTemplate        string    `json:"prompt_template" ch:"prompt_template"`
	PromptTemplateVersion string    `json:"prompt_template_version" ch:"prompt_template_version"`
	ConversationID        string    `json:"conversation_id" ch:"conversation_id"`
	SessionID             string    `json:"session_id" ch:"session_id"`
	FinishReason          string    `json:"finish_reason" ch:"finish_reason"`
	GuardrailState        string    `json:"guardrail_state" ch:"guardrail_state"`
	Status                string    `json:"status" ch:"status"`
	StatusMessage         string    `json:"status_message" ch:"status_message"`
	HasError              bool      `json:"has_error" ch:"has_error"`
	StartTime             time.Time `json:"start_time" ch:"start_time"`
	LatencyMs             float64   `json:"latency_ms" ch:"latency_ms"`
	TTFTMs                float64   `json:"ttft_ms" ch:"ttft_ms"`
	InputTokens           float64   `json:"input_tokens" ch:"input_tokens"`
	OutputTokens          float64   `json:"output_tokens" ch:"output_tokens"`
	TotalTokens           float64   `json:"total_tokens" ch:"total_tokens"`
	CostUSD               float64   `json:"cost_usd" ch:"cost_usd"`
	QualityScore          float64   `json:"quality_score" ch:"quality_score"`
	FeedbackScore         float64   `json:"feedback_score" ch:"feedback_score"`
	QualityBucket         string    `json:"quality_bucket" ch:"quality_bucket"`
}

// AIRunDetail extends AIRunRow with fields only needed by the detail view.
type AIRunDetail struct {
	AIRunRow
	SpanName        string            `json:"span_name" ch:"span_name"`
	ServiceVersion  string            `json:"service_version" ch:"service_version"`
	Environment     string            `json:"environment" ch:"environment"`
	PromptSnippet   string            `json:"prompt_snippet" ch:"prompt_snippet"`
	ResponseSnippet string            `json:"response_snippet" ch:"response_snippet"`
	CacheHit        bool              `json:"cache_hit" ch:"cache_hit"`
	RawAttributes   map[string]string `json:"raw_attributes" ch:"attributes"`
}

// RatioPct computes (errorRuns / totalRuns) * 100, returning 0 on division by zero.
func RatioPct(errorRuns, totalRuns uint64) float64 {
	if totalRuns == 0 {
		return 0
	}
	return (float64(errorRuns) / float64(totalRuns)) * 100
}

// FallbackString returns fallback when value is blank.
func FallbackString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

var emailPattern = regexp.MustCompile(`[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}`)
var secretPattern = regexp.MustCompile(`(?i)(bearer\s+[a-z0-9._\-]+|sk-[a-z0-9]+|api[_-]?key["'=:\s]+[a-z0-9_\-]+)`)

// SanitizeSnippet redacts emails and secrets, collapses whitespace, and truncates.
func SanitizeSnippet(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	clean := strings.TrimSpace(value)
	clean = emailPattern.ReplaceAllString(clean, "[redacted-email]")
	clean = secretPattern.ReplaceAllString(clean, "[redacted-secret]")
	clean = strings.Join(strings.Fields(clean), " ")
	if len(clean) > MaxSnippetLength {
		return clean[:MaxSnippetLength] + "..."
	}
	return clean
}
