package fingerprint

import (
	"fmt"
	"strings"
)

// Calculate produces a composite fingerprint string from resource attributes.
//
// The result has two parts:
//  1. A hierarchy prefix built from identity-relevant attributes only
//     (service.name, deployment.environment, k8s.pod.name, etc.)
//  2. A hash suffix computed from ALL sorted attributes for uniqueness
//
// Example output:
//
//	"cloud.provider=gcp;service.name=frontend;deployment.environment=prod;k8s.pod.name=xyz;hash=7291038456"
//
// The hierarchy prefix keeps related resources sorted together in ClickHouse's
// primary key (ORDER BY ..., fingerprint, ...), enabling efficient
// PREWHERE pruning. The hash suffix ensures two resources with the same
// hierarchy path but different extra attributes still get distinct fingerprints.
func Calculate(attrs map[string]string) string {
	hierarchy := ResourceHierarchy()
	id := hierarchy.Identifier(attrs)

	parts := make([]string, 0, len(id)+1)
	idMap := make(map[string]string, len(id))
	for _, lv := range id {
		parts = append(parts, fmt.Sprintf("%s=%s", lv.Label, lv.Value))
		idMap[lv.Label] = lv.Value
	}

	// We only hash the identified identity attributes. This ensures that
	// unrelated/ephemeral resource attributes (like process.id or a random label)
	// do not cause "fingerprint explosion".
	hash := FingerprintHash(idMap)
	parts = append(parts, fmt.Sprintf("hash=%v", hash))
	return strings.Join(parts, ";")
}
