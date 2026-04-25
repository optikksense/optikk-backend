// Package traceidmatch provides a shared ClickHouse predicate helper so every
// trace-scoped reader normalizes trace_id the same way (logs collapse unset to
// "", legacy spans store 32-char zero hex, hex casing may differ).
package traceidmatch

import "fmt"

// allZeroTraceHex is the 32-char hex form OTLP uses for an unset trace id.
const allZeroTraceHex = "00000000000000000000000000000000"

// WhereTraceIDMatchesCH builds a ClickHouse predicate so trace-scoped reads
// find rows even when logs store "" and legacy spans store all-zero hex, or
// when hex casing differs.
func WhereTraceIDMatchesCH(column, param string) string {
	return fmt.Sprintf(`(
		lowerUTF8(%s) = lowerUTF8(@%s)
		OR (length(@%s) = 0 AND %s = '%s')
		OR (lowerUTF8(@%s) = '%s' AND length(%s) = 0)
	)`, column, param, param, column, allZeroTraceHex, param, allZeroTraceHex, column)
}
