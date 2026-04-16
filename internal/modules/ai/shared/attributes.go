package shared

// AI detection attribute keys — if any of these are present on a span, it is an AI run.
var AIDetectionAttributeKeys = []string{
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

var ProviderAttributeKeys = []string{
	"gen_ai.system",
	"gen_ai.provider.name",
	"llm.provider",
	"ai.provider",
	"optikk.ai.provider",
}

var RequestModelAttributeKeys = []string{
	"gen_ai.request.model",
	"llm.request.model",
	"ai.request.model",
	"ai.model",
	"optikk.ai.model",
	"model.name",
}

var ResponseModelAttributeKeys = []string{
	"gen_ai.response.model",
	"llm.response.model",
	"ai.response.model",
}

var OperationAttributeKeys = []string{
	"gen_ai.operation.name",
	"llm.operation",
	"ai.operation",
	"optikk.ai.operation",
}

var PromptTemplateAttributeKeys = []string{
	"gen_ai.prompt.template.name",
	"llm.prompt.template.name",
	"prompt.template.name",
	"optikk.ai.prompt_template.name",
}

var PromptTemplateVersionAttributeKeys = []string{
	"gen_ai.prompt.template.version",
	"llm.prompt.template.version",
	"prompt.template.version",
	"optikk.ai.prompt_template.version",
}

var ConversationAttributeKeys = []string{
	"gen_ai.conversation.id",
	"llm.conversation.id",
	"conversation.id",
	"optikk.ai.conversation_id",
}

var SessionAttributeKeys = []string{
	"gen_ai.session.id",
	"llm.session.id",
	"session.id",
	"optikk.ai.session_id",
}

var FinishReasonAttributeKeys = []string{
	"gen_ai.response.finish_reasons",
	"llm.finish_reason",
	"finish_reason",
	"optikk.ai.finish_reason",
}

var GuardrailAttributeKeys = []string{
	"optikk.ai.guardrail.status",
	"guardrail.status",
	"gen_ai.guardrail.status",
}

var PromptSnippetAttributeKeys = []string{
	"optikk.ai.prompt_snippet",
	"gen_ai.prompt",
	"llm.prompt",
	"prompt",
	"input",
}

var ResponseSnippetAttributeKeys = []string{
	"optikk.ai.response_snippet",
	"gen_ai.response",
	"llm.response",
	"response",
	"output",
}

var ServiceVersionAttributeKeys = []string{
	"service.version",
	"deployment.version",
	"optikk.ai.service_version",
}

var EnvironmentAttributeKeys = []string{
	"deployment.environment",
	"env",
	"environment",
}

var InputTokensAttributeKeys = []string{
	"gen_ai.usage.input_tokens",
	"llm.usage.prompt_tokens",
	"usage.prompt_tokens",
	"prompt_tokens",
	"optikk.ai.tokens.input",
}

var OutputTokensAttributeKeys = []string{
	"gen_ai.usage.output_tokens",
	"llm.usage.completion_tokens",
	"usage.completion_tokens",
	"completion_tokens",
	"optikk.ai.tokens.output",
}

var TotalTokensAttributeKeys = []string{
	"gen_ai.usage.total_tokens",
	"llm.usage.total_tokens",
	"usage.total_tokens",
	"total_tokens",
	"optikk.ai.tokens.total",
}

var CostAttributeKeys = []string{
	"gen_ai.usage.cost",
	"llm.usage.cost_usd",
	"usage.cost_usd",
	"cost_usd",
	"optikk.ai.cost_usd",
}

var TTFTAttributeKeys = []string{
	"gen_ai.latency.ttft_ms",
	"llm.latency.ttft_ms",
	"ttft_ms",
	"optikk.ai.ttft_ms",
}

var QualityScoreAttributeKeys = []string{
	"optikk.ai.quality.score",
	"quality.score",
	"gen_ai.quality.score",
}

var FeedbackScoreAttributeKeys = []string{
	"optikk.ai.feedback.score",
	"feedback.score",
}

var CacheHitAttributeKeys = []string{
	"optikk.ai.cache.hit",
	"cache.hit",
	"gen_ai.cache.hit",
}
