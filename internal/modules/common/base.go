// Package common contains shared HTTP handler helpers and module dependencies.
// Each domain file exposes a Handler struct wired with a DB connection and
// a tenant-extraction function injected by the parent app package.
package common

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
	"github.com/observability/observability-backend-go/internal/database"
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type GetTenantFunc func(*gin.Context) types.TenantContext

func RespondOK(c *gin.Context, data any) {
	c.JSON(http.StatusOK, types.Success(data))
}

func RespondError(c *gin.Context, status int, code, msg string) {
	if status >= 500 {
		log.Printf("ERROR [%s %s] %s: %s", c.Request.Method, c.Request.URL.Path, code, msg)
	}
	c.JSON(status, types.Failure(code, msg, c.Request.URL.Path))
}

// RespondErrorWithCause logs the underlying error server-side before responding.
// Use this instead of RespondError when you have access to the original error.
func RespondErrorWithCause(c *gin.Context, status int, code, msg string, err error) {
	if err != nil {
		log.Printf("ERROR [%s %s] %s: %s: %v", c.Request.Method, c.Request.URL.Path, code, msg, err)
	} else if status >= 500 {
		log.Printf("ERROR [%s %s] %s: %s", c.Request.Method, c.Request.URL.Path, code, msg)
	}
	c.JSON(status, types.Failure(code, msg, c.Request.URL.Path))
}

func ParseInt64Param(c *gin.Context, key string, fallback int64) int64 {
	if v := c.Query(key); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

const MaxPageSize = 200

func ParseIntParam(c *gin.Context, key string, fallback int) int {
	if v := c.Query(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return fallback
}

func ParsePageSize(c *gin.Context, key string, fallback int) int {
	size := ParseIntParam(c, key, fallback)
	if size > MaxPageSize {
		size = MaxPageSize
	}
	if size <= 0 {
		size = fallback
	}
	return size
}

func ParseListParam(c *gin.Context, key string) []string {
	vals := c.QueryArray(key)
	if len(vals) == 0 {
		vals = c.QueryArray(key + "[]")
	}
	if len(vals) > 0 {
		clean := make([]string, 0, len(vals))
		for _, v := range vals {
			v = strings.TrimSpace(v)
			if v != "" {
				clean = append(clean, v)
			}
		}
		return clean
	}
	raw := c.Query(key)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	clean := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			clean = append(clean, p)
		}
	}
	if len(clean) == 0 {
		return nil
	}
	return clean
}

func ParseRange(c *gin.Context) (int64, int64, error) {
	now := time.Now().UnixMilli()
	end := ParseInt64Param(c, "endTime", 0)
	if end <= 0 {
		end = ParseInt64Param(c, "end", 0)
	}
	start := ParseInt64Param(c, "startTime", 0)
	if start <= 0 {
		start = ParseInt64Param(c, "start", 0)
	}
	if end <= 0 {
		end = now
	}
	if start <= 0 {
		return 0, 0, fmt.Errorf("start time is required")
	}
	if start >= end {
		return 0, 0, fmt.Errorf("start must be before end")
	}
	return start, end, nil
}

func ParseRequiredRange(c *gin.Context) (int64, int64, bool) {
	start, end, err := ParseRange(c)
	if err != nil {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "start and end time params are required")
		return 0, 0, false
	}
	return start, end, true
}

// ParseComparisonRange returns an optional comparison time range.
// If compareStart is provided, it returns the comparison window.
// If only "compareTo" is provided (e.g. "previous_period"), it auto-calculates
// the comparison range based on the primary range.
func ParseComparisonRange(c *gin.Context, startMs, endMs int64) (cmpStart, cmpEnd int64, ok bool) {
	cmpStart = ParseInt64Param(c, "compareStart", 0)
	cmpEnd = ParseInt64Param(c, "compareEnd", 0)
	if cmpStart > 0 && cmpEnd > 0 {
		return cmpStart, cmpEnd, true
	}

	compareTo := c.Query("compareTo")
	duration := endMs - startMs
	switch compareTo {
	case "previous_period":
		return startMs - duration, startMs, true
	case "previous_day":
		return startMs - 86400000, endMs - 86400000, true
	case "previous_week":
		return startMs - 604800000, endMs - 604800000, true
	default:
		return 0, 0, false
	}
}

func ExtractIDParam(c *gin.Context, key string) (int64, error) {
	id, err := strconv.ParseInt(c.Param(key), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid id")
	}
	return id, nil
}

func SqlTime(ms int64) time.Time {
	return dbutil.SqlTime(ms)
}

func NullableString(v string) any {
	return dbutil.NullableString(v)
}

func DefaultString(v, fallback string) string {
	return dbutil.DefaultString(v, fallback)
}

func QueryCount(db *sql.DB, q string, args ...any) int64 {
	return dbutil.QueryCount(database.NewMySQLWrapper(db), q, args...)
}

func InClauseFromStrings(values []string) (string, []any) {
	return dbutil.InClauseFromStrings(values)
}

func Int64FromAny(v any) int64 {
	return dbutil.Int64FromAny(v)
}

func Float64FromAny(v any) float64 {
	return dbutil.Float64FromAny(v)
}

func StringFromAny(v any) string {
	return dbutil.StringFromAny(v)
}

func BoolFromAny(v any) bool {
	return dbutil.BoolFromAny(v)
}

func ToInt64Slice(v any) []int64 {
	return dbutil.ToInt64Slice(v)
}

func NormalizeRows(rows []map[string]any) []map[string]any {
	return dbutil.NormalizeRows(rows)
}

func NormalizeMap(m map[string]any) map[string]any {
	return dbutil.NormalizeMap(m)
}
