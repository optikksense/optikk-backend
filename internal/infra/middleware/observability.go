package middleware

import (
	"log/slog"
	"strconv"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/trace"
)

// ObservabilityMiddleware runs AFTER otelgin so the span context is in
// place when we log. It records Prometheus timing + an `access log` line
// on every HTTP request, bounded by `c.FullPath()` so metric cardinality
// stays route-shape-only (not unbounded path-value).
func ObservabilityMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		metrics.HTTPInFlight.Inc()
		start := time.Now()

		c.Next()

		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		method := c.Request.Method
		status := c.Writer.Status()
		dur := time.Since(start)

		metrics.HTTPDuration.WithLabelValues(route, method).Observe(dur.Seconds())
		metrics.HTTPRequestsTotal.WithLabelValues(route, method, classifyStatus(status)).Inc()
		metrics.HTTPInFlight.Dec()

		emitAccessLog(c, route, method, status, dur)
	}
}

// classifyStatus buckets HTTP codes into the Prom `status_class` label so
// we don't explode series by per-code dimensions. 5xx and 4xx surface
// distinctly; anything else collapses to 2xx/3xx/1xx.
func classifyStatus(s int) string {
	switch {
	case s >= 500:
		return "5xx"
	case s >= 400:
		return "4xx"
	case s >= 300:
		return "3xx"
	case s >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}

func emitAccessLog(c *gin.Context, route, method string, status int, dur time.Duration) {
	ctx := c.Request.Context()
	sc := trace.SpanContextFromContext(ctx)
	attrs := []any{
		slog.String("method", method),
		slog.String("route", route),
		slog.Int("status", status),
		slog.Float64("duration_ms", float64(dur.Nanoseconds())/1e6),
		slog.String("remote_ip", c.ClientIP()),
		slog.String("user_agent", c.Request.UserAgent()),
	}
	if tenant := GetTenant(c); tenant.TeamID != 0 {
		attrs = append(attrs, slog.Int64("team_id", tenant.TeamID))
	}
	if sc.IsValid() {
		attrs = append(attrs,
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	// 5xx responses warrant Warn; the rest are Info so dashboards can
	// filter for unusual HTTP flow without being drowned by 200s.
	msg := "http request " + strconv.Itoa(status)
	if status >= 500 {
		slog.WarnContext(ctx, msg, attrs...)
		return
	}
	slog.InfoContext(ctx, msg, attrs...)
}
