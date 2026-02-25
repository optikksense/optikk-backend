// Package handlers contains HTTP handler functions organized by domain.
// Each domain file exposes a Handler struct wired with a DB connection and
// a tenant-extraction function injected by the parent app package.
package handlers

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
	"github.com/observability/observability-backend-go/internal/database"
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// GetTenantFunc is a function that extracts the tenant context from a request.
// It is provided by the app package to avoid circular imports.
type GetTenantFunc func(*gin.Context) types.TenantContext

// ---- Response helpers -------------------------------------------------------

func RespondOK(c *gin.Context, data any) {
	c.JSON(http.StatusOK, types.Success(data))
}

func RespondError(c *gin.Context, status int, code, msg string) {
	c.JSON(status, types.Failure(code, msg, c.Request.URL.Path))
}

// ---- Query param helpers ----------------------------------------------------

func ParseInt64Param(c *gin.Context, key string, fallback int64) int64 {
	if v := c.Query(key); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func ParseIntParam(c *gin.Context, key string, fallback int) int {
	if v := c.Query(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return fallback
}

func ParseListParam(c *gin.Context, key string) []string {
	vals := c.QueryArray(key)
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

func ParseRange(c *gin.Context, defaultMs int64) (int64, int64) {
	end := ParseInt64Param(c, "endTime", time.Now().UnixMilli())
	start := ParseInt64Param(c, "startTime", end-defaultMs)
	return start, end
}

func ExtractIDParam(c *gin.Context, key string) (int64, error) {
	id, err := strconv.ParseInt(c.Param(key), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid id")
	}
	return id, nil
}

// ---- SQL helpers ------------------------------------------------------------

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

// ---- Type conversion helpers ------------------------------------------------

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
