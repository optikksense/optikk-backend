package database

import (
	"database/sql"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"
)

// MaxQueryRows is the default hard limit on rows returned by QueryMaps to
// prevent unbounded memory allocation on large result sets.
const MaxQueryRows = 10_000

func QueryMaps(db Querier, query string, args ...any) ([]map[string]any, error) {
	return QueryMapsLimit(db, MaxQueryRows, query, args...)
}

func QueryMapsLimit(db Querier, limit int, query string, args ...any) ([]map[string]any, error) {
	if limit <= 0 {
		limit = MaxQueryRows
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }() //nolint:errcheck // best-effort cleanup on deferred close

	cols := rows.Columns()

	result := make([]map[string]any, 0)
	for rows.Next() {
		if len(result) >= limit {
			break
		}
		raw := make([]any, len(cols))
		rawPtrs := make([]any, len(cols))
		for i := range raw {
			rawPtrs[i] = &raw[i]
		}

		rows.Scan(rawPtrs...)

		item := make(map[string]any, len(cols))
		for i, col := range cols {
			item[col] = normalizeValue(raw[i])
		}
		result = append(result, item)
	}

	return result, nil
}

func QueryMap(db Querier, query string, args ...any) (map[string]any, error) {
	items, err := QueryMaps(db, query, args...)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return map[string]any{}, nil
	}
	return items[0], nil
}

func InClause(values []string) (clause string, args []any) {
	if len(values) == 0 {
		return "", nil
	}
	parts := make([]string, len(values))
	args = make([]any, len(values))
	for i, v := range values {
		parts[i] = "?"
		args[i] = v
	}
	return "(" + strings.Join(parts, ",") + ")", args
}

func InClauseInt64(values []int64) (clause string, args []any) {
	if len(values) == 0 {
		return "", nil
	}
	parts := make([]string, len(values))
	args = make([]any, len(values))
	for i, v := range values {
		parts[i] = "?"
		args[i] = v
	}
	return "(" + strings.Join(parts, ",") + ")", args
}

func NamedInClause(prefix string, values []string) (clause string, args map[string]any) {
	if len(values) == 0 {
		return "", nil
	}
	parts := make([]string, len(values))
	args = make(map[string]any, len(values))
	for i, v := range values {
		name := prefix + strconv.Itoa(i)
		parts[i] = "@" + name
		args[name] = v
	}
	return "(" + strings.Join(parts, ",") + ")", args
}

func NamedInClauseInt64(prefix string, values []int64) (clause string, args map[string]any) {
	if len(values) == 0 {
		return "", nil
	}
	parts := make([]string, len(values))
	args = make(map[string]any, len(values))
	for i, v := range values {
		name := prefix + strconv.Itoa(i)
		parts[i] = "@" + name
		args[name] = v
	}
	return "(" + strings.Join(parts, ",") + ")", args
}

func normalizeValue(v any) any {
	switch x := v.(type) {
	case []byte:
		s := strings.TrimSpace(string(x))
		if s == "" {
			return ""
		}
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i
		}
		if u, err := strconv.ParseUint(s, 10, 64); err == nil {
			if u <= math.MaxInt64 {
				return int64(u)
			}
			// Keep very large uint values as string to avoid float precision loss.
			return s
		}
		if strings.ContainsAny(s, ".eE") {
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f
			}
		}
		return s
	case time.Time:
		return x.UTC().Format(time.RFC3339)
	default:
		return v
	}
}

func JSONString(v any) string {
	if v == nil {
		return "{}"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func MustAtoi64(v string, fallback int64) int64 {
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return i
}

func RowsAffected(res sql.Result) int64 {
	if res == nil {
		return 0
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0
	}
	return n
}
