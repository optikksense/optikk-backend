// Package metrics provides Prometheus self-observability instrumentation for
// the Optic backend. It exposes histograms, counters and gauges that capture
// HTTP request behaviour, telemetry ingestion throughput, and runtime health.
//
// Usage:
//
//	metrics.Register()                          // call once at startup
//	r.Use(metrics.GinMiddleware())              // add to Gin middleware chain
//	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
package metrics

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

// -------------------------------------------------------------------
// Metric definitions
// -------------------------------------------------------------------

const namespace = "optic"

// HTTP request metrics.
var (
	// HTTPRequestDuration records latency (in seconds) per method/route/status.
	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "Histogram of HTTP request latencies in seconds.",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"method", "route", "status"},
	)

	// HTTPRequestsTotal counts every HTTP request processed.
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total number of HTTP requests processed.",
		},
		[]string{"method", "route", "status"},
	)

	// HTTPRequestSizeBytes tracks the size of incoming request bodies.
	HTTPRequestSizeBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "http",
			Name:      "request_size_bytes",
			Help:      "Histogram of HTTP request body sizes in bytes.",
			Buckets:   prometheus.ExponentialBuckets(100, 10, 7), // 100B .. 100MB
		},
		[]string{"method", "route"},
	)

	// HTTPResponseSizeBytes tracks the size of outgoing response bodies.
	HTTPResponseSizeBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "http",
			Name:      "response_size_bytes",
			Help:      "Histogram of HTTP response body sizes in bytes.",
			Buckets:   prometheus.ExponentialBuckets(100, 10, 7),
		},
		[]string{"method", "route"},
	)

	// HTTPActiveRequests tracks the number of in-flight HTTP requests.
	HTTPActiveRequests = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "http",
			Name:      "active_requests",
			Help:      "Number of HTTP requests currently being processed.",
		},
	)
)

// Telemetry ingestion counters — callers increment these from the ingestion
// pipeline (e.g. OTLP handler or Kafka consumer).
var (
	IngestionSpansReceived = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "ingestion",
			Name:      "spans_received_total",
			Help:      "Total number of spans received via OTLP ingestion.",
		},
	)

	IngestionLogsReceived = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "ingestion",
			Name:      "logs_received_total",
			Help:      "Total number of log records received via OTLP ingestion.",
		},
	)

	IngestionMetricsReceived = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "ingestion",
			Name:      "metrics_received_total",
			Help:      "Total number of metric data points received via OTLP ingestion.",
		},
	)
)

// Error counters — callers can label errors by category so operators can
// set up alerting on error rates per type.
var (
	ErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "errors",
			Name:      "total",
			Help:      "Total errors encountered, labelled by type (e.g. db, auth, validation, internal).",
		},
		[]string{"type"},
	)
)

// Buffer / queue depth gauges — useful when the server uses internal
// buffered writes (direct ingester or Kafka).
var (
	BufferQueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "buffer",
			Name:      "queue_depth",
			Help:      "Current number of items waiting in an internal buffer queue.",
		},
		[]string{"queue"},
	)
)

// Database connection pool gauges.
var (
	DBActiveConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "db",
			Name:      "active_connections",
			Help:      "Number of active (in-use) database connections.",
		},
		[]string{"backend"}, // "mysql" or "clickhouse"
	)

	DBIdleConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "db",
			Name:      "idle_connections",
			Help:      "Number of idle database connections.",
		},
		[]string{"backend"},
	)
)

// -------------------------------------------------------------------
// Registration
// -------------------------------------------------------------------

// Register registers all Prometheus collectors with the default registry.
// It is safe to call multiple times; duplicate registration panics are
// silently ignored.
func Register() {
	collectors := []prometheus.Collector{
		// HTTP
		HTTPRequestDuration,
		HTTPRequestsTotal,
		HTTPRequestSizeBytes,
		HTTPResponseSizeBytes,
		HTTPActiveRequests,
		// Ingestion
		IngestionSpansReceived,
		IngestionLogsReceived,
		IngestionMetricsReceived,
		// Errors
		ErrorsTotal,
		// Buffer
		BufferQueueDepth,
		// DB
		DBActiveConnections,
		DBIdleConnections,
	}

	for _, c := range collectors {
		// Use MustRegister in production; wrap with a recover guard so that
		// calling Register() twice does not crash.
		_ = prometheus.Register(c)
	}
}

// -------------------------------------------------------------------
// Gin middleware
// -------------------------------------------------------------------

// GinMiddleware returns a Gin handler that records request duration, request
// count, request/response sizes, and tracks active connections for every
// HTTP request.
func GinMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Skip instrumenting the /metrics endpoint itself to avoid
		// self-referential noise.
		if c.Request.URL.Path == "/metrics" {
			c.Next()
			return
		}

		start := time.Now()
		HTTPActiveRequests.Inc()

		// Let the request proceed through the handler chain.
		c.Next()

		HTTPActiveRequests.Dec()

		status := strconv.Itoa(c.Writer.Status())
		route := c.FullPath() // returns the registered route pattern, e.g. "/api/users/:id"
		if route == "" {
			route = "unmatched"
		}
		method := c.Request.Method
		elapsed := time.Since(start).Seconds()

		HTTPRequestDuration.WithLabelValues(method, route, status).Observe(elapsed)
		HTTPRequestsTotal.WithLabelValues(method, route, status).Inc()

		if c.Request.ContentLength > 0 {
			HTTPRequestSizeBytes.WithLabelValues(method, route).Observe(float64(c.Request.ContentLength))
		}
		HTTPResponseSizeBytes.WithLabelValues(method, route).Observe(float64(c.Writer.Size()))
	}
}
