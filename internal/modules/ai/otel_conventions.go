package ai

// OpenTelemetry GenAI Semantic Conventions
// Based on OpenTelemetry Semantic Conventions v1.40.0 for Generative AI
// Reference: https://opentelemetry.io/docs/specs/semconv/gen-ai/

const (
	// Attribute Names - GenAI Request
	AttrGenAIRequestModel            = "gen_ai.request.model"
	AttrGenAIRequestMaxTokens        = "gen_ai.request.max_tokens"
	AttrGenAIRequestTemperature      = "gen_ai.request.temperature"
	AttrGenAIRequestTopP             = "gen_ai.request.top_p"
	AttrGenAIRequestFrequencyPenalty = "gen_ai.request.frequency_penalty"
	AttrGenAIRequestPresencePenalty  = "gen_ai.request.presence_penalty"
	AttrGenAIRequestStopSequences    = "gen_ai.request.stop_sequences"
	AttrGenAIRequestChoiceCount      = "gen_ai.request.choice.count"
	AttrGenAIRequestSeed             = "gen_ai.request.seed"

	// Attribute Names - GenAI Response
	AttrGenAIResponseModel         = "gen_ai.response.model"
	AttrGenAIResponseID            = "gen_ai.response.id"
	AttrGenAIResponseFinishReasons = "gen_ai.response.finish_reasons"

	// Attribute Names - GenAI Usage (Token Metrics)
	AttrGenAIUsageInputTokens              = "gen_ai.usage.input_tokens"
	AttrGenAIUsageOutputTokens             = "gen_ai.usage.output_tokens"
	AttrGenAIUsageCacheReadInputTokens     = "gen_ai.usage.cache_read.input_tokens"
	AttrGenAIUsageCacheCreationInputTokens = "gen_ai.usage.cache_creation.input_tokens"

	// Attribute Names - GenAI Operation
	AttrGenAIOperationName  = "gen_ai.operation.name"
	AttrGenAIOutputType     = "gen_ai.output.type"
	AttrGenAIConversationID = "gen_ai.conversation.id"

	// Attribute Names - Server
	AttrServerAddress = "server.address"
	AttrServerPort    = "server.port"

	// Attribute Names - Error
	AttrErrorType = "error.type"

	// Metric Names - Following OpenTelemetry naming conventions
	MetricGenAIClientOperationDuration = "gen_ai.client.operation.duration"
	MetricGenAIClientTokenUsage        = "gen_ai.client.token.usage"
	MetricGenAIClientOperationCount    = "gen_ai.client.operation.count"
	MetricGenAIClientOperationCost     = "gen_ai.client.operation.cost"

	// Operation Names - Standard GenAI operations
	OperationChat            = "chat"
	OperationTextCompletion  = "text_completion"
	OperationEmbeddings      = "embeddings"
	OperationGenerateContent = "generate_content"

	// Output Types
	OutputTypeText   = "text"
	OutputTypeJSON   = "json"
	OutputTypeImage  = "image"
	OutputTypeSpeech = "speech"

	// Error Types
	ErrorTypeTimeout = "timeout"
	ErrorTypeOther   = "_OTHER"

	// Custom attributes for extended observability (non-standard)
	// These are domain-specific extensions beyond OpenTelemetry spec
	AttrAIModelProvider    = "ai.model.provider"
	AttrAIRequestType      = "ai.request.type"
	AttrAITimeoutFlag      = "ai.timeout"
	AttrAIRetryCount       = "ai.retry.count"
	AttrAICostUSD          = "ai.cost.usd"
	AttrAITokensPrompt     = "ai.tokens.prompt"
	AttrAITokensCompletion = "ai.tokens.completion"
	AttrAITokensSystem     = "ai.tokens.system"
	AttrAICacheHit         = "ai.cache.hit"
	AttrAICacheTokens      = "ai.cache.tokens"
	AttrAIPIIDetected      = "ai.security.pii.detected"
	AttrAIPIICategories    = "ai.security.pii.categories"
	AttrAIGuardrailBlocked = "ai.security.guardrail.blocked"
	AttrAIContentPolicy    = "ai.security.content_policy"

	// Database Column Names - Map OpenTelemetry attributes to ClickHouse columns
	// Standard OpenTelemetry columns
	ColGenAIRequestModel              = "gen_ai_request_model"
	ColServerAddress                  = "server_address"
	ColGenAIOperationName             = "gen_ai_operation_name"
	ColGenAIUsageInputTokens          = "gen_ai_usage_input_tokens"
	ColGenAIUsageOutputTokens         = "gen_ai_usage_output_tokens"
	ColGenAIUsageCacheReadInputTokens = "gen_ai_usage_cache_read_input_tokens"
	ColStatusCode                     = "status_code"

	// Custom extension columns
	ColAITokensSystem             = "ai_tokens_system"
	ColAICostUSD                  = "ai_cost_usd"
	ColAITimeout                  = "ai_timeout"
	ColAICacheHit                 = "ai_cache_hit"
	ColAIRetryCount               = "ai_retry_count"
	ColAISecurityPIIDetected      = "ai_security_pii_detected"
	ColAISecurityPIICategories    = "ai_security_pii_categories"
	ColAISecurityGuardrailBlocked = "ai_security_guardrail_blocked"
	ColAISecurityContentPolicy    = "ai_security_content_policy"
)

const TableMetrics = "observability.metrics"

type OperationType string

const (
	OpTypeChat            OperationType = OperationChat
	OpTypeTextCompletion  OperationType = OperationTextCompletion
	OpTypeEmbeddings      OperationType = OperationEmbeddings
	OpTypeGenerateContent OperationType = OperationGenerateContent
)

type TokenType string

const (
	TokenTypeInput         TokenType = "input"
	TokenTypeOutput        TokenType = "output"
	TokenTypeCacheRead     TokenType = "cache_read"
	TokenTypeCacheCreation TokenType = "cache_creation"
	TokenTypePrompt        TokenType = "prompt"
	TokenTypeCompletion    TokenType = "completion"
	TokenTypeSystem        TokenType = "system"
)

// MetricUnit defines standard units for metrics
const (
	UnitMilliseconds = "ms"
	UnitTokens       = "tokens"
	UnitRequests     = "requests"
	UnitUSD          = "usd"
	UnitPercent      = "percent"
)

type StatusCode string

const (
	StatusOK    StatusCode = "OK"
	StatusError StatusCode = "ERROR"
)

func IsStandardOTelAttribute(attrName string) bool {
	standardAttrs := map[string]bool{
		AttrGenAIRequestModel:       true,
		AttrGenAIRequestMaxTokens:   true,
		AttrGenAIRequestTemperature: true,
		AttrGenAIRequestTopP:        true,
		AttrGenAIResponseModel:      true,
		AttrGenAIResponseID:         true,
		AttrGenAIUsageInputTokens:   true,
		AttrGenAIUsageOutputTokens:  true,
		AttrGenAIOperationName:      true,
		AttrServerAddress:           true,
		AttrServerPort:              true,
		AttrErrorType:               true,
	}
	return standardAttrs[attrName]
}
