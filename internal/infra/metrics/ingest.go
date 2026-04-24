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
)
