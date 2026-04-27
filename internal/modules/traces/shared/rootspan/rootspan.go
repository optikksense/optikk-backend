package rootspan

import "fmt"

const (
	asciiZeroParentID   = "0000000000000000"
	binaryZeroParentHex = "00000000000000000000000000000000"
)

// Condition returns a SQL predicate that matches all supported root-span parent
// representations for observability.signoz_index_v3.parent_span_id.
func Condition(alias string) string {
	prefix := ""
	if alias != "" {
		prefix = alias + "."
	}

	return fmt.Sprintf(
		"(%sparent_span_id = '' OR %sparent_span_id = '%s' OR hex(%sparent_span_id) = '%s')",
		prefix,
		prefix,
		asciiZeroParentID,
		prefix,
		binaryZeroParentHex,
	)
}
