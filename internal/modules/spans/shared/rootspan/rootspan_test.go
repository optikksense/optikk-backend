package rootspan

import (
	"strings"
	"testing"
)

func TestConditionIncludesAllKnownRootRepresentations(t *testing.T) {
	condition := Condition("s")

	checks := []string{
		"s.parent_span_id = ''",
		"s.parent_span_id = '0000000000000000'",
		"hex(s.parent_span_id) = '00000000000000000000000000000000'",
	}

	for _, expected := range checks {
		if !strings.Contains(condition, expected) {
			t.Fatalf("expected condition %q to contain %q", condition, expected)
		}
	}
}

func TestConditionWithoutAliasUsesBareColumnName(t *testing.T) {
	condition := Condition("")

	if strings.Contains(condition, "..") {
		t.Fatalf("unexpected alias formatting in condition %q", condition)
	}
	if !strings.Contains(condition, "parent_span_id = ''") {
		t.Fatalf("expected bare parent_span_id predicate, got %q", condition)
	}
}
