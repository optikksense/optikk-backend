// Package filter holds SQL and OTel helpers shared by saturation/kafka.
package filter

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// MessagingSystemKafka is the OTel messaging.system value for Kafka.
const MessagingSystemKafka = "kafka"

// MaxTopQueues caps the number of rows returned by top-N readers.
const MaxTopQueues = 50

// MetricBucketBounds returns the ts_bucket range for [startMs, endMs].
func MetricBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}

// MetricArgs returns the canonical bind set for Kafka queries.
func MetricArgs(teamID int64, startMs, endMs int64) []any {
	bucketStart, bucketEnd := MetricBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}


func WithMetricNames(args []any, metricNames []string) []any {
	return append(args, clickhouse.Named("metricNames", metricNames))
}

