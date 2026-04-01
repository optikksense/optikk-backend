package analytics

import (
	"fmt"
	"math"
	"strings"
)

// AnalyticsRequest is the unified analytics request for both logs and traces.
type AnalyticsRequest struct {
	Query        string        `json:"query"`
	StartTime    int64         `json:"startTime"`
	EndTime      int64         `json:"endTime"`
	GroupBy      []string      `json:"groupBy"`
	Aggregations []Aggregation `json:"aggregations"`
	OrderBy      string        `json:"orderBy,omitempty"`
	OrderDir     string        `json:"orderDir,omitempty"`
	Limit        int           `json:"limit,omitempty"`
	Step         string        `json:"step,omitempty"`
	VizMode      string        `json:"vizMode,omitempty"`
}

// Aggregation defines a single aggregation to compute.
type Aggregation struct {
	Function string `json:"function"` // count, count_unique, min, max, avg, sum, p50, p75, p90, p95, p99
	Field    string `json:"field,omitempty"`
	Alias    string `json:"alias"`
}

// VizMode constants.
const (
	VizList       = "list"
	VizTimeseries = "timeseries"
	VizTopList    = "toplist"
	VizTable      = "table"
	VizPieChart   = "piechart"
	VizTreeMap    = "treemap"
)

// AnalyticsValueType classifies a cell value.
type AnalyticsValueType string

const (
	ValueString  AnalyticsValueType = "string"
	ValueInteger AnalyticsValueType = "integer"
	ValueNumber  AnalyticsValueType = "number"
)

// AnalyticsCell holds a single value in a result row.
type AnalyticsCell struct {
	Key          string             `json:"key"`
	Type         AnalyticsValueType `json:"type"`
	StringValue  *string            `json:"stringValue,omitempty"`
	IntegerValue *int64             `json:"integerValue,omitempty"`
	NumberValue  *float64           `json:"numberValue,omitempty"`
}

// AnalyticsRow holds one result row.
type AnalyticsRow struct {
	Cells []AnalyticsCell `json:"cells"`
}

// AnalyticsResult is the response from an analytics query.
type AnalyticsResult struct {
	Columns []string       `json:"columns"`
	Rows    []AnalyticsRow `json:"rows"`
}

// ScopeConfig holds scope-specific configuration for query building.
type ScopeConfig struct {
	// Table is the ClickHouse table (e.g. "observability.logs", "observability.spans").
	Table string
	// TableAlias is the alias used in queries (e.g. "" for logs, "s" for spans).
	TableAlias string
	// TimestampColumn is the timestamp column expression.
	TimestampColumn string
	// TimeBucketFunc builds a time bucket expression for a given step.
	TimeBucketFunc func(startMs, endMs int64, step string) string
	// BaseWhereFunc builds the base WHERE clause (team_id, time range, bucket bounds).
	BaseWhereFunc func(teamID int64, startMs, endMs int64) (frag string, args []any)
	// Dimensions maps user-facing dimension names to ClickHouse column expressions.
	Dimensions map[string]string
	// AggFields maps user-facing aggregation field names to ClickHouse column expressions.
	AggFields map[string]string
}

// CellFromValue creates an AnalyticsCell from a typed value.
func CellFromValue(key string, value any) AnalyticsCell {
	switch v := value.(type) {
	case int64:
		return AnalyticsCell{Key: key, Type: ValueInteger, IntegerValue: &v}
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			v = 0
		}
		rounded := math.Round(v*100) / 100
		return AnalyticsCell{Key: key, Type: ValueNumber, NumberValue: &rounded}
	default:
		s := fmt.Sprint(value)
		return AnalyticsCell{Key: key, Type: ValueString, StringValue: &s}
	}
}

// SafeAlias wraps a string in backticks for use as a ClickHouse alias.
func SafeAlias(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "") + "`"
}
