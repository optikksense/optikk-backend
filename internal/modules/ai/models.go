package ai

import "time"

const (
	defaultPageSize       = 25
	maxPageSize           = 200
	defaultBreakdownLimit = 12
	defaultLogLimit       = 50
	maxSnippetLength      = 900
)

var aiDetectionAttributeKeys = []string{
	"gen_ai.system",
	"gen_ai.request.model",
	"gen_ai.response.model",
	"gen_ai.operation.name",
	"gen_ai.usage.input_tokens",
	"gen_ai.usage.output_tokens",
	"gen_ai.usage.total_tokens",
	"llm.provider",
	"llm.request.model",
	"llm.usage.prompt_tokens",
	"llm.usage.completion_tokens",
	"llm.usage.total_tokens",
	"optikk.ai.provider",
	"optikk.ai.model",
	"optikk.ai.operation",
	"optikk.ai.cost_usd",
	"optikk.ai.quality.score",
}

var providerAttributeKeys = []string{
	"gen_ai.system",
	"gen_ai.provider.name",
	"llm.provider",
	"ai.provider",
	"optikk.ai.provider",
}

var requestModelAttributeKeys = []string{
	"gen_ai.request.model",
	"llm.request.model",
	"ai.request.model",
	"ai.model",
	"optikk.ai.model",
	"model.name",
}

var responseModelAttributeKeys = []string{
	"gen_ai.response.model",
	"llm.response.model",
	"ai.response.model",
}

var operationAttributeKeys = []string{
	"gen_ai.operation.name",
	"llm.operation",
	"ai.operation",
	"optikk.ai.operation",
}

var promptTemplateAttributeKeys = []string{
	"gen_ai.prompt.template.name",
	"llm.prompt.template.name",
	"prompt.template.name",
	"optikk.ai.prompt_template.name",
}

var promptTemplateVersionAttributeKeys = []string{
	"gen_ai.prompt.template.version",
	"llm.prompt.template.version",
	"prompt.template.version",
	"optikk.ai.prompt_template.version",
}

var conversationAttributeKeys = []string{
	"gen_ai.conversation.id",
	"llm.conversation.id",
	"conversation.id",
	"optikk.ai.conversation_id",
}

var sessionAttributeKeys = []string{
	"gen_ai.session.id",
	"llm.session.id",
	"session.id",
	"optikk.ai.session_id",
}

var finishReasonAttributeKeys = []string{
	"gen_ai.response.finish_reasons",
	"llm.finish_reason",
	"finish_reason",
	"optikk.ai.finish_reason",
}

var guardrailAttributeKeys = []string{
	"optikk.ai.guardrail.status",
	"guardrail.status",
	"gen_ai.guardrail.status",
}

var promptSnippetAttributeKeys = []string{
	"optikk.ai.prompt_snippet",
	"gen_ai.prompt",
	"llm.prompt",
	"prompt",
	"input",
}

var responseSnippetAttributeKeys = []string{
	"optikk.ai.response_snippet",
	"gen_ai.response",
	"llm.response",
	"response",
	"output",
}

var serviceVersionAttributeKeys = []string{
	"service.version",
	"deployment.version",
	"optikk.ai.service_version",
}

var environmentAttributeKeys = []string{
	"deployment.environment",
	"env",
	"environment",
}

var inputTokensAttributeKeys = []string{
	"gen_ai.usage.input_tokens",
	"llm.usage.prompt_tokens",
	"usage.prompt_tokens",
	"prompt_tokens",
	"optikk.ai.tokens.input",
}

var outputTokensAttributeKeys = []string{
	"gen_ai.usage.output_tokens",
	"llm.usage.completion_tokens",
	"usage.completion_tokens",
	"completion_tokens",
	"optikk.ai.tokens.output",
}

var totalTokensAttributeKeys = []string{
	"gen_ai.usage.total_tokens",
	"llm.usage.total_tokens",
	"usage.total_tokens",
	"total_tokens",
	"optikk.ai.tokens.total",
}

var costAttributeKeys = []string{
	"gen_ai.usage.cost",
	"llm.usage.cost_usd",
	"usage.cost_usd",
	"cost_usd",
	"optikk.ai.cost_usd",
}

var ttftAttributeKeys = []string{
	"gen_ai.latency.ttft_ms",
	"llm.latency.ttft_ms",
	"ttft_ms",
	"optikk.ai.ttft_ms",
}

var qualityScoreAttributeKeys = []string{
	"optikk.ai.quality.score",
	"quality.score",
	"gen_ai.quality.score",
}

var feedbackScoreAttributeKeys = []string{
	"optikk.ai.feedback.score",
	"feedback.score",
}

var cacheHitAttributeKeys = []string{
	"optikk.ai.cache.hit",
	"cache.hit",
	"gen_ai.cache.hit",
}

type queryContext struct {
	teamID int64
	start  int64
	end    int64
	where  string
	args   []any
}

type facetDefinition struct {
	key   string
	label string
	expr  string
}

type qualitySummaryRow struct {
	AvgQualityScore  float64 `json:"avg_quality_score" ch:"avg_quality_score"`
	AvgFeedbackScore float64 `json:"avg_feedback_score" ch:"avg_feedback_score"`
	ScoredRuns       uint64  `json:"scored_runs" ch:"scored_runs"`
	FeedbackRuns     uint64  `json:"feedback_runs" ch:"feedback_runs"`
	GuardrailBlocks  uint64  `json:"guardrail_blocks" ch:"guardrail_blocks"`
}

type alertRow struct {
	ID            int64
	Name          string
	ConditionType string
	RuleState     string
	TargetRef     string
}

type logRow struct {
	Timestamp    time.Time `json:"timestamp" ch:"timestamp"`
	SeverityText string    `json:"severity_text" ch:"severity_text"`
	Body         string    `json:"body" ch:"body"`
	ServiceName  string    `json:"service_name" ch:"service_name"`
	TraceID      string    `json:"trace_id" ch:"trace_id"`
	SpanID       string    `json:"span_id" ch:"span_id"`
}
