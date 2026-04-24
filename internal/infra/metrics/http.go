// Package metrics holds the application-level Prometheus collectors,
// split by subsystem so each file stays under the 200-LOC cap.
//
// Collectors are registered via `promauto` against
// `prometheus.DefaultRegisterer` at package init, so the existing
// `/metrics` endpoint (see internal/app/server/routes.go) exposes them
// without further wiring. The otel-collector's `prometheus` receiver
// scrapes `/metrics` and forwards to Grafana Cloud Mimir.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// HTTP histogram buckets span 5 ms → 5 s — covers healthy API p50 through
// fat-tail p99 on list/facets endpoints. Keep the bucket count reasonable
// (~10) to cap series explosion when multiplied by the label cardinality.
var httpBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}

var (
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "http",
		Name:      "requests_total",
		Help:      "Total HTTP requests served, labelled by route template, method, and status class (2xx/3xx/4xx/5xx).",
	}, []string{"route", "method", "status_class"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "http",
		Name:      "request_duration_seconds",
		Help:      "HTTP request handling duration in seconds, by route + method.",
		Buckets:   httpBuckets,
	}, []string{"route", "method"})

	HTTPInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "optikk",
		Subsystem: "http",
		Name:      "in_flight_requests",
		Help:      "HTTP requests currently being processed.",
	})
)
