package explorer

import (
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// buildAggsRollup constructs aggSpec{alias,expr} for aggregations pushed down
// onto the logs_rollup_v2 cascade. sumState/uniqHLL12State merges go here.
func buildAggsRollup(in []Aggregation) []aggSpec {
	if len(in) == 0 {
		return []aggSpec{{alias: "count", expr: "sumMerge(log_count)"}}
	}
	out := make([]aggSpec, 0, len(in))
	for _, a := range in {
		alias := a.Alias
		if alias == "" {
			alias = a.Fn
		}
		switch a.Fn {
		case "count":
			out = append(out, aggSpec{alias: alias, expr: "sumMerge(log_count)"})
		case "errors", "error_count":
			out = append(out, aggSpec{alias: alias, expr: "sumMerge(error_count)"})
		}
	}
	return out
}

// buildAggsRaw constructs aggSpec for raw logs_v2 reads (full fidelity).
func buildAggsRaw(in []Aggregation) []aggSpec {
	if len(in) == 0 {
		return []aggSpec{{alias: "count", expr: "count()"}}
	}
	out := make([]aggSpec, 0, len(in))
	for _, a := range in {
		alias := a.Alias
		if alias == "" {
			alias = a.Fn
		}
		switch a.Fn {
		case "count":
			out = append(out, aggSpec{alias: alias, expr: "count()"})
		case "errors", "error_count":
			out = append(out, aggSpec{alias: alias, expr: "countIf(severity_bucket >= 4)"})
		case "uniq", "count_distinct":
			if a.Field == "" {
				continue
			}
			out = append(out, aggSpec{alias: alias, expr: fmt.Sprintf("uniq(%s)", safeField(a.Field))})
		case "min":
			if a.Field == "" {
				continue
			}
			out = append(out, aggSpec{alias: alias, expr: fmt.Sprintf("min(%s)", safeField(a.Field))})
		case "max":
			if a.Field == "" {
				continue
			}
			out = append(out, aggSpec{alias: alias, expr: fmt.Sprintf("max(%s)", safeField(a.Field))})
		case "sum":
			if a.Field == "" {
				continue
			}
			out = append(out, aggSpec{alias: alias, expr: fmt.Sprintf("sum(%s)", safeField(a.Field))})
		case "avg":
			if a.Field == "" {
				continue
			}
			out = append(out, aggSpec{alias: alias, expr: fmt.Sprintf("avg(%s)", safeField(a.Field))})
		}
	}
	return out
}

var allowedRawFields = map[string]bool{
	"severity_bucket":  true,
	"severity_number":  true,
	"service":          true,
	"host":             true,
	"pod":              true,
	"container":        true,
	"environment":      true,
	"trace_id":         true,
	"span_id":          true,
	"observed_timestamp": true,
}

func safeField(f string) string {
	if allowedRawFields[f] {
		return f
	}
	return "1"
}

// aggAliasSet returns the set of alias names so mapAnalyticsRow can put them
// in the numeric Values map.
func aggAliasSet(aggs []aggSpec) map[string]bool {
	out := make(map[string]bool, len(aggs))
	for _, a := range aggs {
		out[a.alias] = true
	}
	return out
}

// scanAnyRow scans a CH row into a `[]any` slice for dynamic-shape results.
func scanAnyRow(rows interface {
	Scan(dest ...any) error
}, n int) ([]any, error) {
	values := make([]any, n)
	ptrs := make([]any, n)
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	return values, nil
}

// mapAnalyticsRow converts the raw scanned values into a typed AnalyticsRow.
// time_bucket → TimeBucket; agg aliases → Values; everything else → Group.
func mapAnalyticsRow(cols []string, values []any, aggSet map[string]bool) AnalyticsRow {
	row := AnalyticsRow{
		Group:  make(map[string]string),
		Values: make(map[string]float64),
	}
	for i, col := range cols {
		if i >= len(values) {
			break
		}
		v := values[i]
		if col == "time_bucket" {
			row.TimeBucket = toString(v)
			continue
		}
		if aggSet[col] {
			row.Values[col] = toFloat(v)
			continue
		}
		row.Group[col] = toString(v)
	}
	return row
}

func toString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case time.Time:
		return x.UTC().Format("2006-01-02 15:04:05")
	case *time.Time:
		if x == nil {
			return ""
		}
		return x.UTC().Format("2006-01-02 15:04:05")
	case driver.Valuer:
		val, err := x.Value()
		if err != nil {
			return ""
		}
		return toString(val)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case nil:
		return 0
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case uint:
		return float64(x)
	case uint32:
		return float64(x)
	case uint64:
		return float64(x)
	case uint8:
		return float64(x)
	case string:
		return 0
	}
	return 0
}

var _ = clickhouse.Named
