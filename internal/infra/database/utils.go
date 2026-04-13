package database

import (
	"math"
)

func QueryCount(db Querier, q string, args ...any) int64 {
	var total int64
	row := db.QueryRow(q, args...)
	if err := row.Scan(&total); err != nil {
		return 0
	}
	return total
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
