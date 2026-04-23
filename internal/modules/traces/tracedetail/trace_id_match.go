package tracedetail

import "fmt"

// allZeroTraceHex is the 32-char hex form OTLP uses for an unset trace id. Logs collapse this
// to "" at ingest; older span rows may still store the literal hex.
const allZeroTraceHex = "00000000000000000000000000000000"

// whereTraceIDMatchesCH builds a ClickHouse predicate so trace-scoped reads find rows even when
// logs store "" and legacy spans store all-zero hex, or when hex casing differs.
func whereTraceIDMatchesCH(column, param string) string {
	return fmt.Sprintf(`(
		lowerUTF8(%s) = lowerUTF8(@%s)
		OR (length(@%s) = 0 AND %s = '%s')
		OR (lowerUTF8(@%s) = '%s' AND length(%s) = 0)
	)`, column, param, param, column, allZeroTraceHex, param, allZeroTraceHex, column)
}
