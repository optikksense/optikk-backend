package middleware

import (
	"strconv"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	"github.com/gin-gonic/gin"
)

// HTTPMetricsMiddleware populates the `optikk_http_*` Prometheus metrics.
// Route label is Gin's FullPath() (the template, e.g. `/api/v1/users/:id`)
// so cardinality stays bounded regardless of IDs in the URL. Requests that
// miss every route (404s) are labelled `__unmatched__` so they still show
// up as traffic without exploding series count.
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
