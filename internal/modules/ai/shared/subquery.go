package shared

import (
	"fmt"
	"strings"
)

// AIRunsSubquery returns the ClickHouse subquery that materialises AI run
// columns from the raw spans table using OpenTelemetry semantic convention
// attribute extraction.
func AIRunsSubquery() string {
	return fmt.Sprintf(`(
		SELECT
			s.team_id AS team_id,
			s.ts_bucket_start AS ts_bucket_start,
			s.timestamp AS start_time,
			s.trace_id AS trace_id,
			s.span_id AS span_id,
			concat(s.trace_id, ':', s.span_id) AS run_id,
			s.service_name AS service_name,
			s.name AS span_name,
			if(s.status_code_string = '', if(s.has_error, 'ERROR', 'UNSET'), s.status_code_string) AS status,
			s.status_message AS status_message,
			s.has_error AS has_error,
			s.duration_nano / 1000000.0 AS latency_ms,
			%s AS provider,
			%s AS request_model,
			if(%s = '', %s, %s) AS response_model,
			if(%s = '', s.name, %s) AS operation,
			%s AS prompt_template,
			%s AS prompt_template_version,
			%s AS conversation_id,
			%s AS session_id,
			%s AS finish_reason,
			%s AS guardrail_state,
			%s AS prompt_snippet,
			%s AS response_snippet,
			%s AS service_version,
			%s AS environment,
			%s AS input_tokens,
			%s AS output_tokens,
			multiIf(%s, %s, (%s + %s) > 0, (%s + %s), 0.0) AS total_tokens,
			%s AS cost_usd,
			%s AS ttft_ms,
			%s AS quality_score,
			%s AS feedback_score,
			%s AS cache_hit,
			%s AS quality_bucket,
			s.attributes AS attributes
		FROM observability.spans s
		WHERE %s
	)`,
		StringAttrExpr("s", ProviderAttributeKeys...),
		StringAttrExpr("s", RequestModelAttributeKeys...),
		StringAttrExpr("s", ResponseModelAttributeKeys...),
		StringAttrExpr("s", RequestModelAttributeKeys...),
		StringAttrExpr("s", ResponseModelAttributeKeys...),
		StringAttrExpr("s", OperationAttributeKeys...),
		StringAttrExpr("s", OperationAttributeKeys...),
		StringAttrExpr("s", PromptTemplateAttributeKeys...),
		StringAttrExpr("s", PromptTemplateVersionAttributeKeys...),
		StringAttrExpr("s", ConversationAttributeKeys...),
		StringAttrExpr("s", SessionAttributeKeys...),
		StringAttrExpr("s", FinishReasonAttributeKeys...),
		StringAttrExpr("s", GuardrailAttributeKeys...),
		StringAttrExpr("s", PromptSnippetAttributeKeys...),
		StringAttrExpr("s", ResponseSnippetAttributeKeys...),
		StringAttrExpr("s", ServiceVersionAttributeKeys...),
		StringAttrExpr("s", EnvironmentAttributeKeys...),
		NumberAttrExpr("s", InputTokensAttributeKeys...),
		NumberAttrExpr("s", OutputTokensAttributeKeys...),
		MapContainsExpr("s", TotalTokensAttributeKeys...),
		NumberAttrExpr("s", TotalTokensAttributeKeys...),
		NumberAttrExpr("s", InputTokensAttributeKeys...),
		NumberAttrExpr("s", OutputTokensAttributeKeys...),
		NumberAttrExpr("s", InputTokensAttributeKeys...),
		NumberAttrExpr("s", OutputTokensAttributeKeys...),
		NumberAttrExpr("s", CostAttributeKeys...),
		NumberAttrExpr("s", TTFTAttributeKeys...),
		NumberAttrExpr("s", QualityScoreAttributeKeys...),
		NumberAttrExpr("s", FeedbackScoreAttributeKeys...),
		BoolAttrExpr("s", CacheHitAttributeKeys...),
		QualityBucketExpr(NumberAttrExpr("s", QualityScoreAttributeKeys...)),
		aiDetectionExpr("s"),
	)
}

func aiDetectionExpr(alias string) string {
	parts := make([]string, 0, len(AIDetectionAttributeKeys))
	for _, key := range AIDetectionAttributeKeys {
		parts = append(parts, fmt.Sprintf("mapContains(%s.attributes, '%s')", alias, key))
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

// StringAttrExpr builds a multiIf expression that returns the first non-empty
// string attribute value from the given keys.
func StringAttrExpr(alias string, keys ...string) string {
	if len(keys) == 0 {
		return "''"
	}
	parts := make([]string, 0, (len(keys)*2)+1)
	for _, key := range keys {
		parts = append(parts,
			fmt.Sprintf("mapGet(%s.attributes, '%s') != ''", alias, key),
			fmt.Sprintf("mapGet(%s.attributes, '%s')", alias, key),
		)
	}
	parts = append(parts, "''")
	return "multiIf(" + strings.Join(parts, ", ") + ")"
}

// NumberAttrExpr builds a multiIf expression that returns the first present
// numeric attribute value from the given keys.
func NumberAttrExpr(alias string, keys ...string) string {
	if len(keys) == 0 {
		return "0.0"
	}
	parts := make([]string, 0, (len(keys)*2)+1)
	for _, key := range keys {
		parts = append(parts,
			fmt.Sprintf("mapContains(%s.attributes, '%s')", alias, key),
			fmt.Sprintf("toFloat64OrZero(mapGet(%s.attributes, '%s'))", alias, key),
		)
	}
	parts = append(parts, "0.0")
	return "multiIf(" + strings.Join(parts, ", ") + ")"
}

// BoolAttrExpr builds a multiIf expression that returns true if any of the
// given attribute keys contain a truthy value.
func BoolAttrExpr(alias string, keys ...string) string {
	if len(keys) == 0 {
		return "false"
	}
	parts := make([]string, 0, (len(keys)*2)+1)
	for _, key := range keys {
		parts = append(parts,
			fmt.Sprintf("mapContains(%s.attributes, '%s')", alias, key),
			fmt.Sprintf("lower(mapGet(%s.attributes, '%s')) IN ('1', 'true', 'yes')", alias, key),
		)
	}
	parts = append(parts, "false")
	return "multiIf(" + strings.Join(parts, ", ") + ")"
}

// MapContainsExpr returns a boolean OR expression that checks if any of the
// given keys exist in the attributes map.
func MapContainsExpr(alias string, keys ...string) string {
	if len(keys) == 0 {
		return "false"
	}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("mapContains(%s.attributes, '%s')", alias, key))
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

// QualityBucketExpr maps a quality score expression to a human-readable bucket.
func QualityBucketExpr(scoreExpr string) string {
	return fmt.Sprintf(
		"multiIf(%s >= 0.9, 'excellent', %s >= 0.75, 'good', %s > 0, 'review', 'unscored')",
		scoreExpr, scoreExpr, scoreExpr,
	)
}
