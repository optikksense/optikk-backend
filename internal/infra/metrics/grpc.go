package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// gRPC ingest calls (OTLP export) run orders of magnitude faster than API
// HTTP requests — most are sub-10 ms writes to Kafka. Buckets optimised
// for that range: 0.5 ms → 1 s.
var grpcBuckets = []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1}

var (
	GRPCStarted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "grpc",
		Name:      "started_total",
		Help:      "Total gRPC calls started, by full-method path.",
	}, []string{"method"})

	GRPCHandled = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "grpc",
		Name:      "handled_total",
		Help:      "Total gRPC calls completed, by full-method and response code.",
	}, []string{"method", "code"})

	GRPCDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "grpc",
		Name:      "handling_duration_seconds",
		Help:      "gRPC call handling duration in seconds, by full-method.",
		Buckets:   grpcBuckets,
	}, []string{"method"})
)
