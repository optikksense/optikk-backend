package middleware

import (
	"strconv"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	"github.com/gin-gonic/gin"
)

// HTTPMetricsMiddleware populates `optikk_http_*` Prometheus metrics.
// Route labels use Gin's FullPath() template to bound cardinality.
func HTTPMetricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		metrics.HTTPInFlight.Inc()
		defer metrics.HTTPInFlight.Dec()

		start := time.Now()
		c.Next()

		route := c.FullPath()
		if route == "" {
			route = "__unmatched__"
		}
		method := c.Request.Method
		status := c.Writer.Status()
		metrics.HTTPRequestsTotal.
			WithLabelValues(route, method, statusClass(status)).Inc()
		metrics.HTTPDuration.
			WithLabelValues(route, method).
			Observe(time.Since(start).Seconds())
	}
}

func statusClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	}
	return strconv.Itoa(status)
}
