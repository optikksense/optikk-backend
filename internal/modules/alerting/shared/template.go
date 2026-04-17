package shared

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/channels"
)

func renderSlackMessage(def AlertRuleDefinition, preview NotificationPreview, deepLink string) channels.Rendered {
	return channels.Rendered{
		Title:       preview.Title,
		Body:        preview.Body,
		Severity:    normalizeSeverity(def.Condition.Severity),
		DeepLinkURL: deepLink,
	}
}

func renderRuleNotification(rule *Rule, def AlertRuleDefinition, state string, values map[string]float64, deployHint, deepLink string) channels.Rendered {
	lines := []string{alertSummary(def)}
	if len(values) > 0 {
		keys := make([]string, 0, len(values))
		for key := range values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			lines = append(lines, fmt.Sprintf("%s: %.3f", key, values[key]))
		}
	}
	lines = append(lines, fmt.Sprintf("State: %s", state))
	if deployHint != "" {
		lines = append(lines, fmt.Sprintf("Recent deploy: %s", deployHint))
	}
	if def.Delivery.Note != "" {
		lines = append(lines, fmt.Sprintf("Note: %s", def.Delivery.Note))
	}
	if deepLink != "" {
		lines = append(lines, fmt.Sprintf("Open in Optik: %s", deepLink))
	}
	return channels.Rendered{
		Title:       rule.Name,
		Body:        strings.Join(lines, "\n"),
		Severity:    rule.Severity,
		DeepLinkURL: deepLink,
		Tags:        map[string]string{},
	}
}
