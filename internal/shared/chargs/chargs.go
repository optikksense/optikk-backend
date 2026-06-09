// Package chargs is the single home for the ClickHouse named-arg builders
// shared by range-scoped reader repositories. Signal-specific WHERE-clause
// builders stay in each domain's filter package.
package chargs

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// BucketBounds returns ts_bucket bounds covering [startMs, endMs].
func BucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}

// RangeArgs binds the standard teamID + bucket-bound + time-range args.
func RangeArgs(teamID, startMs, endMs int64) []any {
	bucketStart, bucketEnd := BucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// WithMetricNames appends the metricNames bind to args.
func WithMetricNames(args []any, names []string) []any {
	return append(args, clickhouse.Named("metricNames", names))
}
