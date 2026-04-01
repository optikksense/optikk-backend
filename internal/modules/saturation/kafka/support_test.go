package kafka

import (
	"strings"
	"testing"
)

func TestTopicExprIncludesCanonicalAndLegacyAliases(t *testing.T) {
	expr := topicExpr()

	for _, alias := range topicAttributeAliases {
		if !strings.Contains(expr, alias) {
			t.Fatalf("topicExpr missing alias %q in %q", alias, expr)
		}
	}
}

func TestKafkaFilterClausesAlwaysScopeToKafkaAndAddArgs(t *testing.T) {
	frag, args := kafkaFilterClauses(KafkaFilters{
		Topic: "orders",
		Group: "payments",
	})

	if !strings.Contains(frag, AttrMessagingSystem) {
		t.Fatalf("filter fragment should scope by messaging system: %q", frag)
	}
	if !strings.Contains(frag, MessagingSystemKafka) {
		t.Fatalf("filter fragment should match kafka system: %q", frag)
	}
	if !strings.Contains(frag, "@topicFilter") || !strings.Contains(frag, "@groupFilter") {
		t.Fatalf("filter fragment should include topic and group filters: %q", frag)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 filter args, got %d", len(args))
	}
}

func TestDurationConditionsUseConfiguredOperationAliases(t *testing.T) {
	tests := []struct {
		name       string
		condition  string
		operations []string
	}{
		{name: "publish", condition: publishDurationCondition(), operations: publishOperationAliases},
		{name: "receive", condition: receiveDurationCondition(), operations: receiveOperationAliases},
		{name: "process", condition: processDurationCondition(), operations: processOperationAliases},
	}

	for _, tt := range tests {
		if !strings.Contains(tt.condition, MetricClientOperationDuration) {
			t.Fatalf("%s condition should reference client operation duration: %q", tt.name, tt.condition)
		}
		for _, operation := range tt.operations {
			if !strings.Contains(tt.condition, operation) {
				t.Fatalf("%s condition missing operation alias %q in %q", tt.name, operation, tt.condition)
			}
		}
	}
}
