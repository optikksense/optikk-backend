package database

import "testing"

func TestStringFromAnyDereferencesStringPointers(t *testing.T) {
	value := "inventory-service"

	if got := StringFromAny(&value); got != value {
		t.Fatalf("expected %q, got %q", value, got)
	}
	if got := StringFromAny((*string)(nil)); got != "" {
		t.Fatalf("expected empty string for nil pointer, got %q", got)
	}
}
