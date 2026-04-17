package shared

import (
	"testing"
)

func TestEngineRuleFromDefinitionServiceErrorRate(t *testing.T) {
	rule, err := engineRuleFromDefinition(AlertRuleDefinition{
		Name:        "Checkout errors",
		PresetKind:  PresetServiceErrorRate,
		Scope:       AlertRuleScope{ServiceName: "checkout"},
		Condition:   AlertRuleCondition{Threshold: 7, WindowMinutes: 10, HoldMinutes: 3, Severity: SeverityP1},
		Delivery:    AlertRuleDelivery{SlackWebhookURL: "https://hooks.slack.com/services/T000/B000/XYZ"},
		Enabled:     true,
		Description: "Protect checkout",
	}, nil)
	if err != nil {
		t.Fatalf("engineRuleFromDefinition returned error: %v", err)
	}
	if rule.ConditionType != ConditionErrorRate {
		t.Fatalf("expected condition type %q, got %q", ConditionErrorRate, rule.ConditionType)
	}
	if got := firstWindowMinutes(rule.Windows, 0); got != 10 {
		t.Fatalf("expected 10 minute window, got %d", got)
	}
	if rule.CriticalThreshold != 7 {
		t.Fatalf("expected threshold 7, got %v", rule.CriticalThreshold)
	}
	if rule.ForSecs != 180 {
		t.Fatalf("expected hold 180s, got %d", rule.ForSecs)
	}
}

func TestLegacyDefinitionFromEngineRule(t *testing.T) {
	rule := &Rule{
		Name:              "Legacy HTTP",
		ConditionType:     PresetHTTPCheck,
		TargetRef:         []byte(`{"url":"https://example.com/health","method":"GET","expect_status":200,"timeout_ms":12000}`),
		CriticalThreshold: 0.5,
		Windows:           []Window{{Name: "primary", Secs: 60}},
		Severity:          SeverityP2,
		Enabled:           true,
		SlackWebhookURL:   "https://hooks.slack.com/services/T000/B000/XYZ",
	}
	def := legacyDefinitionFromEngineRule(rule)
	if def.PresetKind != PresetHTTPCheck {
		t.Fatalf("expected preset %q, got %q", PresetHTTPCheck, def.PresetKind)
	}
	if def.Scope.URL != "https://example.com/health" {
		t.Fatalf("expected URL to round-trip, got %q", def.Scope.URL)
	}
	if def.Condition.EvaluationIntervalMinutes != 1 {
		t.Fatalf("expected interval 1 minute, got %d", def.Condition.EvaluationIntervalMinutes)
	}
}

func TestRuleDefinitionFromStoredTargetRef(t *testing.T) {
	rule := &Rule{
		Name:            "AI latency",
		Description:     "Stored normalized definition",
		ConditionType:   PresetAILatency,
		TargetRef:       []byte(`{"preset_kind":"ai_latency","scope":{"service_name":"llm-gateway","provider":"openai","model":"gpt-4.1"},"condition":{"threshold":3000,"window_minutes":5,"hold_minutes":2,"severity":"p2"},"delivery_note":"Page the AI owner"}`),
		SlackWebhookURL: "https://hooks.slack.com/services/T000/B000/XYZ",
		Enabled:         true,
	}
	def := ruleDefinitionFromRow(rule)
	if def.PresetKind != PresetAILatency {
		t.Fatalf("expected stored preset %q, got %q", PresetAILatency, def.PresetKind)
	}
	if def.Scope.Provider != "openai" {
		t.Fatalf("expected provider openai, got %q", def.Scope.Provider)
	}
	if def.Delivery.Note != "Page the AI owner" {
		t.Fatalf("expected delivery note to round-trip, got %q", def.Delivery.Note)
	}
}
