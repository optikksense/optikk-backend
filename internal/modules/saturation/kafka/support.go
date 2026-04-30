package kafka

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

const (
	MessagingSystemKafka = "kafka"
	MaxTopQueues         = 50
	partitionLagTopN     = 200
)

var (
	ProducerMetrics    = producerMetricAliases
	ConsumerMetrics    = consumerMetricAliases
	ProcessMetrics     = processMetricAliases
	ConsumerLagMetrics = consumerLagMetricAliases
	RebalanceMetrics   = rebalanceMetricAliases
	DurationMetrics    = durationMetricAliases
)

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func metricBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}

func metricArgs(teamID int64, startMs, endMs int64) []any {
	bucketStart, bucketEnd := metricBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func withMetricName(args []any, metricName string) []any {
	return append(args, clickhouse.Named("metricName", metricName))
}

func withMetricNames(args []any, metricNames []string) []any {
	return append(args, clickhouse.Named("metricNames", metricNames))
}

func withOpAliases(args []any, opAliases []string) []any {
	return append(args, clickhouse.Named("opAliases", opAliases))
}

// withStepSec binds the display-bucket grain (1m / 5m / 1h / 1d, window-adaptive)
// for SQL-side `intDiv(toUnixTimestamp(timestamp), @stepSec) * @stepSec` grouping.
// Used by the histogram-series queries so CH does the (display_bucket, dim)
// folding (`max(hist_buckets)` + `sumForEach(hist_counts)`) for any window;
// service stays a thin per-row mapper over the merged histograms.
func withStepSec(args []any, startMs, endMs int64) []any {
	step := int64(timebucket.DisplayGrain(endMs - startMs).Seconds())
	if step <= 0 {
		step = 60
	}
	return append(args, clickhouse.Named("stepSec", step))
}
