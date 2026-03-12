package database

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

func SqlTime(ms int64) time.Time {
	return time.UnixMilli(ms).UTC()
}

func NullableString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func DefaultString(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func QueryCount(db Querier, q string, args ...any) int64 {
	var total int64
	row := db.QueryRow(q, args...)
	if err := row.Scan(&total); err != nil {
		return 0
	}
	return total
}

func InClauseFromStrings(values []string) (string, []any) {
	return InClause(values)
}

func Int64FromAny(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case uint64:
		return int64(n)
	case uint32:
		return int64(n)
	case float64:
		return int64(n)
	case []byte:
		return toInt64(string(n), 0)
	case string:
		return toInt64(n, 0)
	default:
		return 0
	}
}

func Float64FromAny(v any) float64 {
	clean := func(f float64) float64 {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0
		}
		return f
	}

	switch n := v.(type) {
	case float64:
		return clean(n)
	case *float64:
		if n == nil {
			return 0
		}
		return clean(*n)
	case float32:
		return clean(float64(n))
	case *float32:
		if n == nil {
			return 0
		}
		return clean(float64(*n))
	case int64:
		return float64(n)
	case *int64:
		if n == nil {
			return 0
		}
		return float64(*n)
	case int:
		return float64(n)
	case *int:
		if n == nil {
			return 0
		}
		return float64(*n)
	case uint64:
		return float64(n)
	case *uint64:
		if n == nil {
			return 0
		}
		return float64(*n)
	case uint32:
		return float64(n)
	case *uint32:
		if n == nil {
			return 0
		}
		return float64(*n)
	case []byte:
		f, err := strconv.ParseFloat(string(n), 64)
		if err != nil {
			return 0
		}
		return clean(f)
	case string:
		f, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0
		}
		return clean(f)
	default:
		return 0
	}
}

func StringFromAny(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case *string:
		if s == nil {
			return ""
		}
		return *s
	case []byte:
		return string(s)
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

func BoolFromAny(v any) bool {
	switch b := v.(type) {
	case bool:
		return b
	case int64:
		return b != 0
	case int:
		return b != 0
	case float64:
		return b != 0
	case []byte:
		return string(b) == "1" || strings.EqualFold(string(b), "true")
	case string:
		return b == "1" || strings.EqualFold(b, "true")
	default:
		return false
	}
}

func ToInt64Slice(v any) []int64 {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]int64, 0, len(arr))
	for _, item := range arr {
		id := Int64FromAny(item)
		if id > 0 {
			out = append(out, id)
		}
	}
	return out
}

func NormalizeRows(rows []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		item := map[string]any{}
		for k, v := range row {
			item[k] = sanitizeValue(v)
		}
		out = append(out, item)
	}
	return out
}

func NormalizeMap(m map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		out[k] = sanitizeValue(v)
	}
	return out
}

// sanitizeValue replaces NaN/Inf float64 with 0 so JSON encoding doesn't fail.
func sanitizeValue(v any) any {
	switch n := v.(type) {
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return 0.0
		}
		return math.Round(n*100) / 100
	case float32:
		if math.IsNaN(float64(n)) || math.IsInf(float64(n), 0) {
			return 0.0
		}
		return float32(math.Round(float64(n)*100) / 100)
	}
	return v
}

func toInt64(s string, fallback int64) int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fallback
	}
	return v
}

func NullableStringFromAny(v any) *string {
	if v == nil {
		return nil
	}
	s := StringFromAny(v)
	if s == "" {
		return nil
	}
	return &s
}

func NullableFloat64FromAny(v any) *float64 {
	if v == nil {
		return nil
	}
	if n, ok := v.(*float64); ok {
		if n == nil {
			return nil
		}
		f := Float64FromAny(*n)
		return &f
	}
	if n, ok := v.(*float32); ok {
		if n == nil {
			return nil
		}
		f := Float64FromAny(*n)
		return &f
	}
	f := Float64FromAny(v)
	return &f
}

func TimeFromAny(v any) time.Time {
	if t, ok := v.(time.Time); ok {
		return t
	}
	if b, ok := v.([]byte); ok {
		return TimeFromAny(string(b))
	}
	if n, ok := v.(int64); ok {
		if n > 1e18 {
			return time.Unix(0, n).UTC()
		}
		if n > 1e15 {
			return time.UnixMilli(n / 1000).UTC()
		}
		if n > 1e12 {
			return time.UnixMilli(n).UTC()
		}
		return time.Unix(n, 0).UTC()
	}
	if n, ok := v.(float64); ok {
		return TimeFromAny(int64(n))
	}
	if s, ok := v.(string); ok {
		s = strings.TrimSpace(s)
		if s == "" {
			return time.Time{}
		}
		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05.999999",
			"2006-01-02 15:04:05",
			"2006-01-02 15:04:05Z07:00",
		}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, s); err == nil {
				return t.UTC()
			}
		}
		if strings.HasSuffix(s, "Z") {
			if t, err := time.Parse("2006-01-02 15:04:05.999999999Z", s); err == nil {
				return t.UTC()
			}
		}
		if unixMs, err := strconv.ParseInt(s, 10, 64); err == nil {
			return TimeFromAny(unixMs)
		}
	}
	return time.Time{}
}

func NullableTimeFromAny(v any) *time.Time {
	if v == nil {
		return nil
	}
	if t, ok := v.(time.Time); ok {
		return &t
	}
	if s, ok := v.(string); ok && s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return &t
		}
	}
	return nil
}
