package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "optikk_http_requests_total",
			Help: "Total HTTP requests by method, path, and status code.",
		},
		[]string{"method", "path", "status"},
	)

	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "optikk_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"method", "path"},
	)

	ClickHouseQueryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "optikk_clickhouse_query_duration_seconds",
			Help:    "ClickHouse query duration in seconds.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		},
		[]string{"operation"},
	)

	DispatcherQueueSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "optikk_dispatcher_queue_size",
			Help: "Current items in dispatcher channel.",
		},
		[]string{"type", "signal"},
	)

	DispatcherDropsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "optikk_dispatcher_drops_total",
			Help: "Total dispatcher batch drops.",
		},
		[]string{"signal"},
	)

	WSConnectionsActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "optikk_ws_connections_active",
			Help: "Active WebSocket connections.",
		},
	)
)

func init() {
	prometheus.MustRegister(
		HTTPRequestsTotal,
		HTTPRequestDuration,
		ClickHouseQueryDuration,
		DispatcherQueueSize,
		DispatcherDropsTotal,
		WSConnectionsActive,
	)
}
