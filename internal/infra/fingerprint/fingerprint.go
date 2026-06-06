package fingerprint

import (
	"fmt"
	"strings"
)

// Calculate produces a composite fingerprint string from resource attributes
// using a hierarchy prefix and a hash suffix.
func Calculate(attrs map[string]string) string {
	hierarchy := ResourceHierarchy()
	id := hierarchy.Identifier(attrs)

	parts := make([]string, 0, len(id)+1)
	idMap := make(map[string]string, len(id))
	for _, lv := range id {
		parts = append(parts, fmt.Sprintf("%s=%s", lv.Label, lv.Value))
		idMap[lv.Label] = lv.Value
	}

	// We only hash identity attributes to prevent fingerprint explosion
	// from unrelated or ephemeral resource attributes.
	hash := FingerprintHash(idMap)
	parts = append(parts, fmt.Sprintf("hash=%v", hash))
	return strings.Join(parts, ";")
}

// CalculateHash produces a stable uint64 hash of identity attributes.
func CalculateHash(attrs map[string]string) uint64 {
	hierarchy := ResourceHierarchy()
	id := hierarchy.Identifier(attrs)
	idMap := make(map[string]string, len(id))
	for _, lv := range id {
		idMap[lv.Label] = lv.Value
	}
	return FingerprintHash(idMap)
}
