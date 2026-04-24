package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Ingest counters are the customer-product primary KPI: records/sec
// received on /otlp/v1/{traces,logs,metrics}. Signal label lets us split
// throughput by pipeline.
var (
	IngestRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "records_total",
		Help:      "OTLP records accepted into the ingest pipeline, by signal (traces/logs/metrics) and result (ok/err).",
	}, []string{"signal", "result"})

	IngestRecordBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "record_bytes_total",
		Help:      "OTLP record payload bytes ingested, by signal.",
	}, []string{"signal"})

	// HandlerPublishDuration covers handler → Kafka (the critical gRPC-side
	// latency the OTLP caller experiences). Buckets span 1ms → 10s; shared
	// across logs/spans/metrics.
	HandlerPublishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "handler_publish_duration_seconds",
		Help:      "OTLP handler → Kafka PublishBatch latency, labeled signal + result (ok/err).",
		Buckets: []float64{
			0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
		},
	}, []string{"signal", "result"})

	// MapperDuration covers OTLP → internal Row conversion. Cheap when
	// payloads are small (<1 ms); histograms catch misshapen large payloads.
	MapperDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "mapper_duration_seconds",
		Help:      "MapRequest OTLP → internal Row wall-clock latency, by signal.",
		Buckets: []float64{
			0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
		},
	}, []string{"signal"})

	// MapperRowsPerRequest histograms the fan-out factor — one gRPC request
	// produces N Rows. Useful for detecting noisy tenants.
	MapperRowsPerRequest = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "mapper_rows_per_request",
		Help:      "Rows produced by MapRequest per OTLP RPC, by signal.",
		Buckets:   []float64{1, 10, 100, 500, 1000, 5000, 10_000, 50_000, 100_000},
	}, []string{"signal"})

	// MapperAttrsDropped counts attribute-map entries dropped by the
	// per-record cap (deterministic key-sort truncation). Replaces the
	// per-record warn log that would flood stderr at 200k rps.
	MapperAttrsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "mapper_attrs_dropped_total",
		Help:      "Attribute-map entries dropped by the per-record cap, by signal.",
	}, []string{"signal"})
)
