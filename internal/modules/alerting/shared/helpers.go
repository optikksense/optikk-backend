package shared

import (
	"encoding/json"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/channels"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/evaluators"
)

// Helpers used across the decomposed submodules (rules/, incidents/,
// silences/, slack/, engine/). Extracted from the former handler/service/
// repository god files so subpackages can depend on a single shared surface
// rather than reaching into cross-concern code.

// Itoa is a minimal int64→string for log + idempotency-key composition.
// Exported because engine.Dispatcher and rules.Service both render alert IDs.
func Itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// AdaptRule projects alerting.Rule to the evaluators.Rule shape. Used by the
// evaluator loop and the backtest runner.
func AdaptRule(r *Rule) evaluators.Rule {
	ws := make([]evaluators.Window, len(r.Windows))
	for i, w := range r.Windows {
		ws[i] = evaluators.Window{Name: w.Name, Secs: w.Secs}
	}
	return evaluators.Rule{
		ID:            r.ID,
		TeamID:        r.TeamID,
		ConditionType: r.ConditionType,
		TargetRef:     r.TargetRef,
		TargetService: TargetServiceFromRef(r.TargetRef),
		GroupBy:       r.GroupBy,
		Windows:       ws,
	}
}

// TargetServiceFromRef extracts {"service_name":"..."} from a rule's TargetRef.
func TargetServiceFromRef(raw json.RawMessage) string {
	m := targetRefMap(raw)
	for _, k := range []string{"service_name", "service", "serviceName"} {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}

// BuildAIWhereFromRef builds the WHERE clause + args for an AI metric query
// given a rule's TargetRef (service/provider/model/prompt_template filters).
func BuildAIWhereFromRef(teamID int64, targetRef json.RawMessage) (string, []any) {
	where := "s.team_id = @teamID"
	args := []any{clickhouse.Named("teamID", uint32(teamID))} //nolint:gosec

	if len(targetRef) == 0 {
		return where, args
	}
	m := targetRefMap(targetRef)

	for _, k := range []string{"service_name", "service", "serviceName"} {
		if v, ok := m[k].(string); ok && v != "" {
			where += " AND s.service_name = @serviceName"
			args = append(args, clickhouse.Named("serviceName", v))
			break
		}
	}

	if v, ok := m["provider"].(string); ok && v != "" {
		where += " AND (mapGet(s.attributes, 'optikk.ai.provider') = @provider OR mapGet(s.attributes, 'llm.provider') = @provider OR mapGet(s.attributes, 'gen_ai.system') = @provider)"
		args = append(args, clickhouse.Named("provider", v))
	}

	for _, k := range []string{"model", "request_model", "requestModel"} {
		if v, ok := m[k].(string); ok && v != "" {
			where += " AND (mapGet(s.attributes, 'optikk.ai.model') = @model OR mapGet(s.attributes, 'llm.request.model') = @model OR mapGet(s.attributes, 'gen_ai.request.model') = @model)"
			args = append(args, clickhouse.Named("model", v))
			break
		}
	}

	for _, k := range []string{"prompt_template", "promptTemplate"} {
		if v, ok := m[k].(string); ok && v != "" {
			where += " AND (mapGet(s.attributes, 'optikk.ai.prompt_template.name') = @promptTemplate OR mapGet(s.attributes, 'gen_ai.prompt.template.name') = @promptTemplate)"
			args = append(args, clickhouse.Named("promptTemplate", v))
			break
		}
	}

	return where, args
}

// RenderRuleNotification re-exports the unexported render helper so engine's
// dispatcher can build notifications. Returns the same channels.Rendered the
// Slack channel consumes downstream.
func RenderRuleNotification(rule *Rule, def AlertRuleDefinition, state string, values map[string]float64, deployHint, deepLink string) channels.Rendered {
	return renderRuleNotification(rule, def, state, values, deployHint, deepLink)
}

// RuleDefinitionFromRow is the read-side projection from the stored Rule back
// to an AlertRuleDefinition (the presentation shape).
func RuleDefinitionFromRow(rule *Rule) AlertRuleDefinition {
	return ruleDefinitionFromRow(rule)
}

// ValidateAlertRuleDefinition is the shared validator used by rules service
// PreviewRule / CreateRule / UpdateRule plus slack.TestSlack.
func ValidateAlertRuleDefinition(def AlertRuleDefinition) error {
	return validateAlertRuleDefinition(def)
}

// NormalizeAlertRuleDefinition sets defaults + canonical fields on a definition.
func NormalizeAlertRuleDefinition(def AlertRuleDefinition) AlertRuleDefinition {
	return normalizeAlertRuleDefinition(def)
}

// EngineRuleFromDefinition materialises an engine-side *Rule from a definition.
func EngineRuleFromDefinition(def AlertRuleDefinition, existing *Rule) (*Rule, error) {
	return engineRuleFromDefinition(def, existing)
}

// PreviewNotification produces a title/body preview for a rule definition.
func PreviewNotification(def AlertRuleDefinition) NotificationPreview {
	return previewNotification(def)
}

// AlertSummary returns the one-line summary rendered in list/detail responses.
func AlertSummary(def AlertRuleDefinition) string {
	return alertSummary(def)
}

// TrimString removes surrounding whitespace; re-exported so subpackages avoid
// importing strings in tiny wrappers.
func TrimString(s string) string { return strings.TrimSpace(s) }
