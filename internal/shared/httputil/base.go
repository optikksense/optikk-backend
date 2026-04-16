// Package common contains shared HTTP handler helpers and module dependencies.
// Each domain file exposes a Handler struct wired with a DB connection and
// a tenant-extraction function injected by the parent app package.
package httputil

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	types "github.com/Optikk-Org/optikk-backend/internal/shared/contracts"
	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
)

type GetTenantFunc func(*gin.Context) types.TenantContext

func RespondOK(c *gin.Context, data any) {
	c.JSON(http.StatusOK, Success(data))
}

func RespondError(c *gin.Context, status int, code, msg string) {
	if status >= 500 {
		slog.Error("request error",
			slog.String("code", code), slog.String("msg", msg),
			slog.String("method", c.Request.Method), slog.String("path", c.Request.URL.Path))
	}
	c.JSON(status, Failure(code, msg, c.Request.URL.Path))
}

// RespondErrorWithCause logs the underlying error server-side before responding.
// The cause is appended to the client-facing message so the caller knows why it failed.
// Use this instead of RespondError when you have access to the original error.
func RespondErrorWithCause(c *gin.Context, status int, code, msg string, err error) {
	if err != nil {
		slog.Error("request error",
			slog.String("code", code), slog.String("msg", msg),
			slog.String("method", c.Request.Method), slog.String("path", c.Request.URL.Path),
			slog.Any("error", err))
		msg = fmt.Sprintf("%s: %s", msg, dbutil.SanitizeError(err))
	} else if status >= 500 {
		slog.Error("request error",
			slog.String("code", code), slog.String("msg", msg),
			slog.String("method", c.Request.Method), slog.String("path", c.Request.URL.Path))
	}
	c.JSON(status, Failure(code, msg, c.Request.URL.Path))
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

func ParseFloatParam(c *gin.Context, key string, fallback float64) float64 {
	if v := c.Query(key); v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
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

func ParseRange(c *gin.Context) (startMs, endMs int64, err error) {
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
		return 0, 0, errors.New("start time is required")
	}
	if start >= end {
		return 0, 0, errors.New("start must be before end")
	}
	return start, end, nil
}

func ParseRequiredRange(c *gin.Context) (startMs, endMs int64, ok bool) {
	start, end, err := ParseRange(c)
	if err != nil {
		RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "start and end time params are required")
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

// ComparisonResponse wraps a primary result with an optional comparison result.
type ComparisonResponse struct {
	Data       any `json:"data"`
	Comparison any `json:"comparison,omitempty"`
}

// WithComparison executes a query for the primary range and optionally for the comparison range.
// Returns a ComparisonResponse containing both results.
func WithComparison(c *gin.Context, startMs, endMs int64, queryFn func(s, e int64) (any, error)) (ComparisonResponse, error) {
	primary, err := queryFn(startMs, endMs)
	if err != nil {
		return ComparisonResponse{}, err
	}

	resp := ComparisonResponse{Data: primary}

	cmpStart, cmpEnd, hasCmp := ParseComparisonRange(c, startMs, endMs)
	if hasCmp {
		comparison, err := queryFn(cmpStart, cmpEnd)
		if err != nil {
			//nolint:nilerr // non-fatal: return primary result without comparison data
			return resp, nil
		}
		resp.Comparison = comparison
	}

	return resp, nil
}

func ExtractIDParam(c *gin.Context, key string) (int64, error) {
	id, err := strconv.ParseInt(c.Param(key), 10, 64)
	if err != nil {
		return 0, errors.New("invalid id")
	}
	return id, nil
}
