package queryparser

import "strings"

// AISchema resolves field names for the normalized AI runs subquery.
type AISchema struct{}

var aiFieldMap = map[string]FieldInfo{
	"run_id":                  {Column: "ai.run_id", Type: FieldString},
	"trace_id":                {Column: "ai.trace_id", Type: FieldString},
	"span_id":                 {Column: "ai.span_id", Type: FieldString},
	"service":                 {Column: "ai.service_name", Type: FieldString},
	"service_name":            {Column: "ai.service_name", Type: FieldString},
	"provider":                {Column: "ai.provider", Type: FieldString},
	"model":                   {Column: "ai.request_model", Type: FieldString},
	"request_model":           {Column: "ai.request_model", Type: FieldString},
	"response_model":          {Column: "ai.response_model", Type: FieldString},
	"operation":               {Column: "ai.operation", Type: FieldString},
	"prompt_template":         {Column: "ai.prompt_template", Type: FieldString},
	"prompt_template_version": {Column: "ai.prompt_template_version", Type: FieldString},
	"conversation":            {Column: "ai.conversation_id", Type: FieldString},
	"conversation_id":         {Column: "ai.conversation_id", Type: FieldString},
	"session":                 {Column: "ai.session_id", Type: FieldString},
	"session_id":              {Column: "ai.session_id", Type: FieldString},
	"status":                  {Column: "ai.status", Type: FieldString},
	"finish_reason":           {Column: "ai.finish_reason", Type: FieldString},
	"guardrail_state":         {Column: "ai.guardrail_state", Type: FieldString},
	"quality_bucket":          {Column: "ai.quality_bucket", Type: FieldString},
	"latency":                 {Column: "ai.latency_ms", Type: FieldNumber},
	"latency_ms":              {Column: "ai.latency_ms", Type: FieldNumber},
	"ttft":                    {Column: "ai.ttft_ms", Type: FieldNumber},
	"ttft_ms":                 {Column: "ai.ttft_ms", Type: FieldNumber},
	"cost":                    {Column: "ai.cost_usd", Type: FieldNumber},
	"cost_usd":                {Column: "ai.cost_usd", Type: FieldNumber},
	"quality_score":           {Column: "ai.quality_score", Type: FieldNumber},
	"feedback_score":          {Column: "ai.feedback_score", Type: FieldNumber},
	"input_tokens":            {Column: "ai.input_tokens", Type: FieldNumber},
	"output_tokens":           {Column: "ai.output_tokens", Type: FieldNumber},
	"total_tokens":            {Column: "ai.total_tokens", Type: FieldNumber},
	"cache_hit":               {Column: "ai.cache_hit", Type: FieldBool},
	"has_error":               {Column: "ai.has_error", Type: FieldBool},
}

func (AISchema) Resolve(field string) (FieldInfo, bool) {
	lower := strings.ToLower(field)

	if info, ok := aiFieldMap[lower]; ok {
		return info, true
	}

	if strings.HasPrefix(field, "@") {
		attrKey := field[1:]
		return FieldInfo{
			Column: "mapGet(ai.attributes, '" + escapeSingleQuote(attrKey) + "')",
			Type:   FieldString,
		}, true
	}

	return FieldInfo{}, false
}

func (AISchema) FreeTextColumns() []string {
	return []string{
		"ai.run_id",
		"ai.trace_id",
		"ai.service_name",
		"ai.provider",
		"ai.request_model",
		"ai.operation",
		"ai.prompt_template",
		"ai.span_name",
	}
}

func (AISchema) TableAlias() string { return "ai" }
