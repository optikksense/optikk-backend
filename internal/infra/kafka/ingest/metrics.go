package ingest

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus collectors for the generic ingest pipeline. Every metric carries
// a `signal` label so logs/spans/metrics share the same series namespace and
// the Grafana dashboard panels can slice per signal without duplicated PromQL.
//
// Buckets are chosen to span the two regimes we operate in:
//   - Durations use a 1ms → 30s exponential spread so tick-grain flushes (a
//     few ms) and retry-stalled writes (tens of seconds) both land in
//     non-saturating buckets.
//   - Row/byte buckets mirror the default AccumulatorConfig thresholds
//     (10k rows / 16 MiB) with a long right tail for debugging
//     misconfigured-large batches.
var (
	FlushDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "worker_flush_duration_seconds",
		Help:      "End-to-end Writer.Write latency per flushed batch, by signal + result (ok/err).",
		Buckets: []float64{
			0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
			1, 2.5, 5, 10, 30,
		},
	}, []string{"signal", "result"})

	FlushRows = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "worker_flush_rows",
		Help:      "Rows per flushed batch, labeled by the accumulator trigger that fired.",
		Buckets:   []float64{100, 500, 1000, 2500, 5000, 10_000, 25_000, 50_000, 100_000},
	}, []string{"signal", "reason"})

	BatchBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "worker_flush_bytes",
		Help:      "Accumulated bytes per flushed batch.",
		Buckets: []float64{
			64 * 1024, 256 * 1024, 1 << 20, 4 << 20, 8 << 20, 16 << 20, 32 << 20, 64 << 20,
		},
	}, []string{"signal"})

	WorkerQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "worker_queue_depth",
		Help:      "Current per-partition worker inbox depth, sampled on every tick.",
	}, []string{"signal", "partition"})

	PausedPartitions = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "worker_paused_partitions",
		Help:      "Number of partitions currently paused for backpressure.",
	}, []string{"signal"})

	CHInsertDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "writer_ch_insert_duration_seconds",
		Help:      "PrepareBatch → Append loop → Send wall-clock latency for one CH attempt.",
		Buckets: []float64{
			0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
			1, 2.5, 5, 10, 30,
		},
	}, []string{"signal", "result"})

	CHRowsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "writer_ch_rows_total",
		Help:      "Rows handed to ClickHouse in attempt() calls, by result.",
	}, []string{"signal", "result"})

	RetryAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "writer_retry_attempts_total",
		Help:      "Writer attempts beyond the first. attempt=1 is the baseline (free); this counter catches 2nd..Nth tries.",
	}, []string{"signal"})

	DLQSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "writer_dlq_sent_total",
		Help:      "Batches routed to the DLQ topic after exhausting retries, by failure reason code.",
	}, []string{"signal", "reason_code"})

	DLQPublishFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "writer_dlq_publish_failed_total",
		Help:      "DLQ publish errors. Rising values indicate a broken DLQ broker or misconfigured topic.",
	}, []string{"signal"})
)
