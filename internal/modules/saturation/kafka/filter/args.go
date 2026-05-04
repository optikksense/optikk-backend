// Package filter holds SQL helpers, OTel constants/aliases, and fold helpers
// shared by every saturation/kafka submodule. Mirrors the role of
// internal/modules/saturation/database/filter for the kafka surface.
package filter

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// MessagingSystemKafka is the fixed value of the OTel `messaging.system`
// attribute that scopes every kafka panel — every reader OR-matches it.
const MessagingSystemKafka = "kafka"

// MaxTopQueues caps the number of rows returned by top-N readers.
const MaxTopQueues = 50

// MetricBucketBounds returns the inclusive ts_bucket range covering
// [startMs, endMs] at the canonical 5-minute grain. Adds one BucketSeconds to
// the upper bound so the rightmost bucket is included.
func MetricBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}

// MetricArgs returns the canonical bind set for every kafka reader
// (`@teamID`, `@bucketStart`, `@bucketEnd`, `@start`, `@end`).
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

func WithMetricName(args []any, metricName string) []any {
	return append(args, clickhouse.Named("metricName", metricName))
}

func WithMetricNames(args []any, metricNames []string) []any {
	return append(args, clickhouse.Named("metricNames", metricNames))
}

func WithOpAliases(args []any, opAliases []string) []any {
	return append(args, clickhouse.Named("opAliases", opAliases))
}
