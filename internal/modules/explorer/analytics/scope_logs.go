package analytics

import (
	"fmt"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

// LogsScopeConfig returns the ScopeConfig for logs analytics.
func LogsScopeConfig() ScopeConfig {
	return ScopeConfig{
		Table:           "observability.logs",
		TableAlias:      "",
		TimestampColumn: "toDateTime(intDiv(timestamp, 1000000000))",
		TimeBucketFunc:  logsTimeBucket,
		BaseWhereFunc:   logsBaseWhere,
		Dimensions:      logsDimensions,
		AggFields:       logsAggFields,
	}
}

var logsDimensions = map[string]string{
	"severity":    "severity_text",
	"level":       "severity_text",
	"service":     "service",
	"host":        "host",
	"pod":         "pod",
	"container":   "container",
	"environment": "environment",
	"scope_name":  "scope_name",
	"logger":      "scope_name",
	"trace_id":    "trace_id",
}

var logsAggFields = map[string]string{
	"severity_number": "severity_number",
}

func logsTimeBucket(startMs, endMs int64, step string) string {
	tsExpr := "toDateTime(intDiv(timestamp, 1000000000))"
	if strings.TrimSpace(step) != "" {
		strat := utils.ByName(step)
		bucketFunc := extractBucketFunc(strat.GetBucketExpression())
		return fmt.Sprintf("formatDateTime(%s(%s), '%%Y-%%m-%%d %%H:%%i:00')", bucketFunc, tsExpr)
	}
	return utils.ExprForColumn(startMs, endMs, tsExpr)
}

func logsBaseWhere(teamID int64, startMs, endMs int64) (string, []any) {
	startNs := uint64(startMs) * 1_000_000 //nolint:gosec // G115
	endNs := uint64(endMs) * 1_000_000     //nolint:gosec // G115
	startBucket := utils.LogsBucketStart(startMs / 1000)
	endBucket := utils.LogsBucketStart(endMs / 1000)

	frag := "team_id = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ?"
	args := []any{uint32(teamID), startBucket, endBucket, startNs, endNs} //nolint:gosec // G115
	return frag, args
}

func extractBucketFunc(expr string) string {
	start := strings.Index(expr, "(") + 1
	end := strings.Index(expr[start:], "(")
	if start > 0 && end > 0 {
		return expr[start : start+end]
	}
	return "toStartOfMinute"
}
