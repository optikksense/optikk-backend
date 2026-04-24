package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// DB histogram buckets: 1 ms → 1 s. Most ClickHouse indexed reads land
// under 50 ms; MySQL user/team lookups under 10 ms. The long tail (1 s+)
// gets bucketed into +Inf so Grafana can flag it.
var dbBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1}

var (
	DBQueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "db",
		Name:      "query_duration_seconds",
		Help:      "DB query duration in seconds, by engine (clickhouse/mysql) and operation label.",
		Buckets:   dbBuckets,
	}, []string{"system", "op"})

	DBQueriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "db",
		Name:      "queries_total",
		Help:      "Total DB queries executed, by engine, operation, and result (ok/err).",
	}, []string{"system", "op", "result"})
)
