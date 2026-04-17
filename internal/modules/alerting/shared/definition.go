package shared

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	PresetServiceErrorRate = "service_error_rate"
	PresetSLOBurnRate      = "slo_burn_rate"
	PresetHTTPCheck        = "http_check"
	PresetAILatency        = "ai_latency"
	PresetAIErrorRate      = "ai_error_rate"
	PresetAICostSpike      = "ai_cost_spike"
	PresetAIQualityDrop    = "ai_quality_drop"
)

type AlertRuleDefinition struct {
	Name        string             `json:"name"`
	Description string             `json:"description,omitempty"`
	PresetKind  string             `json:"preset_kind"`
	Scope       AlertRuleScope     `json:"scope"`
	Condition   AlertRuleCondition `json:"condition"`
	Delivery    AlertRuleDelivery  `json:"delivery"`
	Enabled     bool               `json:"enabled"`
}

type AlertRuleScope struct {
	ServiceName         string `json:"service_name,omitempty"`
	Environment         string `json:"environment,omitempty"`
	SLOID               string `json:"slo_id,omitempty"`
	URL                 string `json:"url,omitempty"`
	Method              string `json:"method,omitempty"`
	ExpectStatus        int    `json:"expect_status,omitempty"`
	TimeoutMs           int    `json:"timeout_ms,omitempty"`
	FollowRedirects     bool   `json:"follow_redirects,omitempty"`
	ExpectBodySubstring string `json:"expect_body_substring,omitempty"`
	Provider            string `json:"provider,omitempty"`
	Model               string `json:"model,omitempty"`
	PromptTemplate      string `json:"prompt_template,omitempty"`
}

type AlertRuleCondition struct {
	Threshold                 float64 `json:"threshold"`
	WindowMinutes             int     `json:"window_minutes,omitempty"`
	HoldMinutes               int     `json:"hold_minutes,omitempty"`
	Severity                  string  `json:"severity,omitempty"`
	Sensitivity               string  `json:"sensitivity,omitempty"`
	EvaluationIntervalMinutes int     `json:"evaluation_interval_minutes,omitempty"`
}

type AlertRuleDelivery struct {
	SlackWebhookURL string `json:"slack_webhook_url"`
	Note            string `json:"note,omitempty"`
}

type storedRuleDefinition struct {
	PresetKind   string             `json:"preset_kind"`
	Scope        AlertRuleScope     `json:"scope"`
	Condition    AlertRuleCondition `json:"condition"`
	DeliveryNote string             `json:"delivery_note,omitempty"`
}

type RuleEnginePreview struct {
	ConditionType     string   `json:"condition_type"`
	Operator          string   `json:"operator"`
	Windows           []Window `json:"windows"`
	CriticalThreshold float64  `json:"critical_threshold"`
	RecoveryThreshold *float64 `json:"recovery_threshold,omitempty"`
	ForSecs           int64    `json:"for_secs"`
	RecoverForSecs    int64    `json:"recover_for_secs"`
	NoDataSecs        int64    `json:"no_data_secs"`
	Severity          string   `json:"severity"`
}

type NotificationPreview struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

func validateAlertRuleDefinition(def AlertRuleDefinition) error {
	if strings.TrimSpace(def.Name) == "" {
		return fmt.Errorf("alerting: rule name required")
	}
	if strings.TrimSpace(def.PresetKind) == "" {
		return fmt.Errorf("alerting: preset_kind required")
	}
	if strings.TrimSpace(def.Delivery.SlackWebhookURL) == "" {
		return fmt.Errorf("alerting: slack webhook required")
	}
	if !strings.HasPrefix(def.Delivery.SlackWebhookURL, "https://hooks.slack.com/") {
		return fmt.Errorf("alerting: invalid slack webhook")
	}

	switch def.PresetKind {
	case PresetServiceErrorRate:
		if strings.TrimSpace(def.Scope.ServiceName) == "" {
			return fmt.Errorf("alerting: service_name required")
		}
	case PresetSLOBurnRate:
		if strings.TrimSpace(def.Scope.ServiceName) == "" {
			return fmt.Errorf("alerting: service_name required")
		}
		if strings.TrimSpace(def.Scope.SLOID) == "" {
			return fmt.Errorf("alerting: slo_id required")
		}
	case PresetHTTPCheck:
		if strings.TrimSpace(def.Scope.URL) == "" {
			return fmt.Errorf("alerting: url required")
		}
	case PresetAILatency, PresetAIErrorRate, PresetAICostSpike, PresetAIQualityDrop:
		if strings.TrimSpace(def.Scope.ServiceName) == "" &&
			strings.TrimSpace(def.Scope.Provider) == "" &&
			strings.TrimSpace(def.Scope.Model) == "" &&
			strings.TrimSpace(def.Scope.PromptTemplate) == "" {
			return fmt.Errorf("alerting: at least one ai scope filter required")
		}
	default:
		return ErrUnsupportedCondition
	}
	return nil
}

func normalizeAlertRuleDefinition(def AlertRuleDefinition) AlertRuleDefinition {
	out := def
	out.Name = strings.TrimSpace(out.Name)
	out.Description = strings.TrimSpace(out.Description)
	out.Delivery.SlackWebhookURL = strings.TrimSpace(out.Delivery.SlackWebhookURL)
	out.Delivery.Note = strings.TrimSpace(out.Delivery.Note)
	out.Scope.ServiceName = strings.TrimSpace(out.Scope.ServiceName)
	out.Scope.Environment = strings.TrimSpace(out.Scope.Environment)
	out.Scope.SLOID = strings.TrimSpace(out.Scope.SLOID)
	out.Scope.URL = strings.TrimSpace(out.Scope.URL)
	out.Scope.Method = strings.ToUpper(strings.TrimSpace(out.Scope.Method))
	out.Scope.ExpectBodySubstring = strings.TrimSpace(out.Scope.ExpectBodySubstring)
	out.Scope.Provider = strings.TrimSpace(out.Scope.Provider)
	out.Scope.Model = strings.TrimSpace(out.Scope.Model)
	out.Scope.PromptTemplate = strings.TrimSpace(out.Scope.PromptTemplate)
	out.Condition.Severity = normalizeSeverity(out.Condition.Severity)

	switch out.PresetKind {
	case PresetServiceErrorRate:
		if out.Condition.WindowMinutes <= 0 {
			out.Condition.WindowMinutes = 5
		}
		if out.Condition.HoldMinutes <= 0 {
			out.Condition.HoldMinutes = 2
		}
		if out.Condition.Threshold <= 0 {
			out.Condition.Threshold = 5
		}
	case PresetSLOBurnRate:
		if out.Condition.Severity == "" {
			out.Condition.Severity = SeverityP2
		}
		if out.Condition.Sensitivity == "" {
			out.Condition.Sensitivity = "balanced"
		}
	case PresetHTTPCheck:
		if out.Scope.Method == "" {
			out.Scope.Method = "GET"
		}
		if out.Scope.ExpectStatus == 0 {
			out.Scope.ExpectStatus = 200
		}
		if out.Scope.TimeoutMs <= 0 {
			out.Scope.TimeoutMs = 10000
		}
		if out.Scope.TimeoutMs > 60000 {
			out.Scope.TimeoutMs = 60000
		}
		if out.Condition.EvaluationIntervalMinutes <= 0 {
			out.Condition.EvaluationIntervalMinutes = 1
		}
		if out.Condition.Threshold <= 0 {
			out.Condition.Threshold = 0.5
		}
	case PresetAILatency:
		if out.Condition.WindowMinutes <= 0 {
			out.Condition.WindowMinutes = 5
		}
		if out.Condition.HoldMinutes <= 0 {
			out.Condition.HoldMinutes = 2
		}
		if out.Condition.Threshold <= 0 {
			out.Condition.Threshold = 2500
		}
	case PresetAIErrorRate:
		if out.Condition.WindowMinutes <= 0 {
			out.Condition.WindowMinutes = 5
		}
		if out.Condition.HoldMinutes <= 0 {
			out.Condition.HoldMinutes = 2
		}
		if out.Condition.Threshold <= 0 {
			out.Condition.Threshold = 5
		}
	case PresetAICostSpike:
		if out.Condition.WindowMinutes <= 0 {
			out.Condition.WindowMinutes = 15
		}
		if out.Condition.HoldMinutes <= 0 {
			out.Condition.HoldMinutes = 5
		}
		if out.Condition.Threshold <= 0 {
			out.Condition.Threshold = 50
		}
	case PresetAIQualityDrop:
		if out.Condition.WindowMinutes <= 0 {
			out.Condition.WindowMinutes = 15
		}
		if out.Condition.HoldMinutes <= 0 {
			out.Condition.HoldMinutes = 5
		}
		if out.Condition.Threshold <= 0 {
			out.Condition.Threshold = 0.7
		}
	}
	return out
}

func engineRuleFromDefinition(def AlertRuleDefinition, existing *Rule) (*Rule, error) {
	normalized := normalizeAlertRuleDefinition(def)
	if err := validateAlertRuleDefinition(normalized); err != nil {
		return nil, err
	}

	rule := &Rule{}
	if existing != nil {
		*rule = *existing
	}

	rule.Name = normalized.Name
	rule.Description = normalized.Description
	rule.GroupBy = nil
	rule.WarnThreshold = nil
	rule.NotifyTemplate = ""
	rule.MaxNotifsPerHour = 4
	rule.SlackWebhookURL = normalized.Delivery.SlackWebhookURL
	rule.Enabled = normalized.Enabled
	rule.Severity = normalized.Condition.Severity
	rule.KeepAliveSecs = 1800
	rule.NoDataSecs = 600

	raw, err := json.Marshal(storedRuleDefinition{
		PresetKind:   normalized.PresetKind,
		Scope:        normalized.Scope,
		Condition:    normalized.Condition,
		DeliveryNote: normalized.Delivery.Note,
	})
	if err != nil {
		return nil, fmt.Errorf("alerting: marshal definition: %w", err)
	}
	rule.TargetRef = raw

	switch normalized.PresetKind {
	case PresetServiceErrorRate:
		rule.ConditionType = ConditionErrorRate
		rule.Operator = OpGT
		rule.Windows = []Window{{Name: "primary", Secs: int64(normalized.Condition.WindowMinutes) * 60}}
		rule.CriticalThreshold = normalized.Condition.Threshold
		rule.RecoveryThreshold = thresholdRecoveryPointer(rule.Operator, normalized.Condition.Threshold)
		rule.ForSecs = int64(normalized.Condition.HoldMinutes) * 60
		rule.RecoverForSecs = int64(normalized.Condition.HoldMinutes) * 60
	case PresetSLOBurnRate:
		rule.ConditionType = ConditionSLOBurnRate
		rule.Operator = OpGT
		threshold, windowMinutes, holdMinutes := sloPresetDefaults(normalized.Condition.Sensitivity)
		rule.Windows = []Window{{Name: "primary", Secs: int64(windowMinutes) * 60}}
		rule.CriticalThreshold = threshold
		rule.RecoveryThreshold = thresholdRecoveryPointer(rule.Operator, threshold)
		rule.ForSecs = int64(holdMinutes) * 60
		rule.RecoverForSecs = int64(holdMinutes) * 60
	case PresetHTTPCheck:
		rule.ConditionType = PresetHTTPCheck
		rule.Operator = OpGT
		rule.Windows = []Window{{Name: "primary", Secs: int64(normalized.Condition.EvaluationIntervalMinutes) * 60}}
		rule.CriticalThreshold = normalized.Condition.Threshold
		rule.RecoveryThreshold = thresholdRecoveryPointer(rule.Operator, normalized.Condition.Threshold)
		rule.ForSecs = 0
		rule.RecoverForSecs = 0
		rule.NoDataSecs = int64(normalized.Condition.EvaluationIntervalMinutes) * 120
	case PresetAILatency, PresetAIErrorRate, PresetAICostSpike:
		rule.ConditionType = normalized.PresetKind
		rule.Operator = OpGT
		rule.Windows = []Window{{Name: "primary", Secs: int64(normalized.Condition.WindowMinutes) * 60}}
		rule.CriticalThreshold = normalized.Condition.Threshold
		rule.RecoveryThreshold = thresholdRecoveryPointer(rule.Operator, normalized.Condition.Threshold)
		rule.ForSecs = int64(normalized.Condition.HoldMinutes) * 60
		rule.RecoverForSecs = int64(normalized.Condition.HoldMinutes) * 60
	case PresetAIQualityDrop:
		rule.ConditionType = normalized.PresetKind
		rule.Operator = OpLT
		rule.Windows = []Window{{Name: "primary", Secs: int64(normalized.Condition.WindowMinutes) * 60}}
		rule.CriticalThreshold = normalized.Condition.Threshold
		rule.RecoveryThreshold = thresholdRecoveryPointer(rule.Operator, normalized.Condition.Threshold)
		rule.ForSecs = int64(normalized.Condition.HoldMinutes) * 60
		rule.RecoverForSecs = int64(normalized.Condition.HoldMinutes) * 60
	default:
		return nil, ErrUnsupportedCondition
	}

	return rule, nil
}

func ruleDefinitionFromRow(rule *Rule) AlertRuleDefinition {
	legacy := legacyDefinitionFromEngineRule(rule)
	legacy.Name = rule.Name
	legacy.Description = rule.Description
	legacy.Delivery.SlackWebhookURL = rule.SlackWebhookURL
	legacy.Enabled = rule.Enabled

	var stored storedRuleDefinition
	if len(rule.TargetRef) == 0 || json.Unmarshal(rule.TargetRef, &stored) != nil || stored.PresetKind == "" {
		return legacy
	}

	def := AlertRuleDefinition{
		Name:        rule.Name,
		Description: rule.Description,
		PresetKind:  stored.PresetKind,
		Scope:       stored.Scope,
		Condition:   stored.Condition,
		Delivery: AlertRuleDelivery{
			SlackWebhookURL: rule.SlackWebhookURL,
			Note:            stored.DeliveryNote,
		},
		Enabled: rule.Enabled,
	}
	return normalizeAlertRuleDefinition(def)
}

func legacyDefinitionFromEngineRule(rule *Rule) AlertRuleDefinition {
	def := AlertRuleDefinition{
		Name:        rule.Name,
		Description: rule.Description,
		Enabled:     rule.Enabled,
		Delivery: AlertRuleDelivery{
			SlackWebhookURL: rule.SlackWebhookURL,
		},
	}

	legacy := targetRefMap(rule.TargetRef)
	serviceName := firstString(legacy, "service_name", "service", "serviceName")

	switch rule.ConditionType {
	case ConditionErrorRate:
		def.PresetKind = PresetServiceErrorRate
		def.Scope.ServiceName = serviceName
		def.Scope.Environment = firstString(legacy, "env", "environment")
		def.Condition.Threshold = rule.CriticalThreshold
		def.Condition.WindowMinutes = firstWindowMinutes(rule.Windows, 5)
		def.Condition.HoldMinutes = secondsToMinutes(rule.ForSecs, 2)
		def.Condition.Severity = normalizeSeverity(rule.Severity)
	case ConditionSLOBurnRate:
		def.PresetKind = PresetSLOBurnRate
		def.Scope.ServiceName = serviceName
		def.Scope.SLOID = firstString(legacy, "slo_id", "sloId")
		def.Condition.Severity = normalizeSeverity(rule.Severity)
		def.Condition.Sensitivity = sensitivityFromRule(rule)
	case PresetHTTPCheck:
		def.PresetKind = PresetHTTPCheck
		def.Scope.URL = firstString(legacy, "url")
		def.Scope.Method = firstString(legacy, "method")
		def.Scope.ExpectStatus = int(firstNumber(legacy, 200, "expect_status"))
		def.Scope.TimeoutMs = int(firstNumber(legacy, 10000, "timeout_ms"))
		def.Scope.FollowRedirects = firstBool(legacy, false, "follow_redirects")
		def.Scope.ExpectBodySubstring = firstString(legacy, "expect_body_substring")
		def.Condition.Threshold = nonZeroFloat(rule.CriticalThreshold, 0.5)
		def.Condition.EvaluationIntervalMinutes = firstWindowMinutes(rule.Windows, 1)
		def.Condition.Severity = normalizeSeverity(rule.Severity)
	case PresetAILatency, PresetAIErrorRate, PresetAICostSpike, PresetAIQualityDrop:
		def.PresetKind = rule.ConditionType
		def.Scope.ServiceName = serviceName
		def.Scope.Provider = firstString(legacy, "provider")
		def.Scope.Model = firstString(legacy, "model", "request_model", "requestModel")
		def.Scope.PromptTemplate = firstString(legacy, "prompt_template", "promptTemplate")
		def.Condition.Threshold = rule.CriticalThreshold
		def.Condition.WindowMinutes = firstWindowMinutes(rule.Windows, 5)
		def.Condition.HoldMinutes = secondsToMinutes(rule.ForSecs, 2)
		def.Condition.Severity = normalizeSeverity(rule.Severity)
	default:
		def.PresetKind = PresetServiceErrorRate
		def.Scope.ServiceName = serviceName
		def.Condition.Threshold = rule.CriticalThreshold
		def.Condition.WindowMinutes = firstWindowMinutes(rule.Windows, 5)
		def.Condition.HoldMinutes = secondsToMinutes(rule.ForSecs, 2)
		def.Condition.Severity = normalizeSeverity(rule.Severity)
	}

	return normalizeAlertRuleDefinition(def)
}

func previewNotification(def AlertRuleDefinition) NotificationPreview {
	normalized := normalizeAlertRuleDefinition(def)
	value := normalized.Condition.Threshold
	switch normalized.PresetKind {
	case PresetHTTPCheck:
		value = 1
	case PresetAIQualityDrop:
		value = normalized.Condition.Threshold * 0.8
	default:
		value = normalized.Condition.Threshold * 1.2
	}

	title := normalized.Name
	if strings.TrimSpace(title) == "" {
		title = presetDisplayName(normalized.PresetKind)
	}

	lines := []string{
		alertSummary(normalized),
		fmt.Sprintf("Observed value: %.2f", value),
		fmt.Sprintf("Severity: %s", normalizeSeverity(normalized.Condition.Severity)),
	}
	if normalized.Delivery.Note != "" {
		lines = append(lines, fmt.Sprintf("Note: %s", normalized.Delivery.Note))
	}
	return NotificationPreview{
		Title: title,
		Body:  strings.Join(lines, "\n"),
	}
}

func alertSummary(def AlertRuleDefinition) string {
	normalized := normalizeAlertRuleDefinition(def)
	switch normalized.PresetKind {
	case PresetServiceErrorRate:
		return fmt.Sprintf(
			"Alert when %s error rate exceeds %.2f%% for %d minute(s) over a %d minute window.",
			fallbackString(normalized.Scope.ServiceName, "the selected service"),
			normalized.Condition.Threshold,
			normalized.Condition.HoldMinutes,
			normalized.Condition.WindowMinutes,
		)
	case PresetSLOBurnRate:
		return fmt.Sprintf(
			"Alert when %s SLO %s breaches the %s sensitivity profile.",
			fallbackString(normalized.Scope.ServiceName, "the selected service"),
			fallbackString(normalized.Scope.SLOID, "target"),
			normalized.Condition.Sensitivity,
		)
	case PresetHTTPCheck:
		return fmt.Sprintf(
			"Alert when the HTTP check for %s fails.",
			fallbackString(normalized.Scope.URL, "the configured endpoint"),
		)
	case PresetAILatency:
		return fmt.Sprintf(
			"Alert when AI latency exceeds %.2f ms in a %d minute window.",
			normalized.Condition.Threshold,
			normalized.Condition.WindowMinutes,
		)
	case PresetAIErrorRate:
		return fmt.Sprintf(
			"Alert when AI error rate exceeds %.2f%% in a %d minute window.",
			normalized.Condition.Threshold,
			normalized.Condition.WindowMinutes,
		)
	case PresetAICostSpike:
		return fmt.Sprintf(
			"Alert when AI cost exceeds %.2f USD in a %d minute window.",
			normalized.Condition.Threshold,
			normalized.Condition.WindowMinutes,
		)
	case PresetAIQualityDrop:
		return fmt.Sprintf(
			"Alert when AI quality score drops below %.2f in a %d minute window.",
			normalized.Condition.Threshold,
			normalized.Condition.WindowMinutes,
		)
	default:
		return "Alert configuration preview unavailable."
	}
}

func presetDisplayName(kind string) string {
	switch kind {
	case PresetServiceErrorRate:
		return "Service error rate"
	case PresetSLOBurnRate:
		return "SLO burn rate"
	case PresetHTTPCheck:
		return "HTTP check"
	case PresetAILatency:
		return "AI latency"
	case PresetAIErrorRate:
		return "AI error rate"
	case PresetAICostSpike:
		return "AI cost spike"
	case PresetAIQualityDrop:
		return "AI quality drop"
	default:
		return kind
	}
}

func sensitivityFromRule(rule *Rule) string {
	windowMinutes := firstWindowMinutes(rule.Windows, 5)
	switch {
	case windowMinutes <= 5:
		return "fast"
	case windowMinutes >= 30:
		return "slow"
	default:
		return "balanced"
	}
}

func sloPresetDefaults(sensitivity string) (threshold float64, windowMinutes int, holdMinutes int) {
	switch strings.ToLower(strings.TrimSpace(sensitivity)) {
	case "fast":
		return 5, 5, 2
	case "slow":
		return 1, 30, 10
	default:
		return 2, 15, 5
	}
}

func thresholdRecoveryPointer(operator string, threshold float64) *float64 {
	var value float64
	switch operator {
	case OpLT, OpLTE:
		value = threshold * 1.1
	default:
		value = threshold * 0.8
	}
	return &value
}

func firstWindowMinutes(windows []Window, fallback int) int {
	if len(windows) == 0 || windows[0].Secs <= 0 {
		return fallback
	}
	return int(windows[0].Secs / 60)
}

func secondsToMinutes(seconds int64, fallback int) int {
	if seconds <= 0 {
		return fallback
	}
	return int(seconds / 60)
}

func normalizeSeverity(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case SeverityP1, SeverityP2, SeverityP3, SeverityP4, SeverityP5:
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return SeverityP2
	}
}

func targetRefMap(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return map[string]any{}
	}
	scope, ok := parsed["scope"].(map[string]any)
	if ok {
		for key, value := range scope {
			if _, exists := parsed[key]; !exists {
				parsed[key] = value
			}
		}
	}
	return parsed
}

func firstString(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := m[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstBool(m map[string]any, fallback bool, keys ...string) bool {
	for _, key := range keys {
		if value, ok := m[key].(bool); ok {
			return value
		}
	}
	return fallback
}

func firstNumber(m map[string]any, fallback float64, keys ...string) float64 {
	for _, key := range keys {
		switch value := m[key].(type) {
		case float64:
			return value
		case int:
			return float64(value)
		case int64:
			return float64(value)
		}
	}
	return fallback
}

func fallbackString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func nonZeroFloat(value, fallback float64) float64 {
	if value == 0 {
		return fallback
	}
	return value
}
