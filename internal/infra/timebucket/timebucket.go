// Package timebucket is the single source of truth for every bucket value in the system. Writers and readers call the same helpers; ClickHouse never computes a bucket itself.
package timebucket

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const BucketSeconds int64 = 300

// BucketStart truncates a Unix-second timestamp down to the start of its 5-minute bucket.
// Returned as UInt32 so it round-trips losslessly through the CH ts_bucket column.
func BucketStart(unixSeconds int64) uint32 {
	return uint32((unixSeconds / BucketSeconds) * BucketSeconds)
}

// DisplayBucket buckets a row timestamp into a display-time bucket sized for the requested query window. Grain: ≤3h → 1m, ≤24h → 5m, ≤7d → 1h, otherwise 1d.
func DisplayBucket(rowUnixSeconds int64, windowMs int64) time.Time {
	return bucketAt(rowUnixSeconds, displayGrain(windowMs))
}

func DisplayGrain(windowMs int64) time.Duration {
	return displayGrain(windowMs)
}

func displayGrain(windowMs int64) time.Duration {
	w := time.Duration(windowMs) * time.Millisecond
	switch {
	case w <= 3*time.Hour:
		return time.Minute
	case w <= 24*time.Hour:
		return 5 * time.Minute
	case w <= 7*24*time.Hour:
		return time.Hour
	default:
		return 24 * time.Hour
	}
}

func bucketAt(unixSeconds int64, grain time.Duration) time.Time {
	return time.Unix(unixSeconds, 0).UTC().Truncate(grain)
}

// DisplayGrainSQL returns the fastest CH SQL fragment for the
// window-adaptive display grain, dispatching to the specific
// toStartOf{Minute,FiveMinutes,Hour,Day} function — 10–15% faster
// than the general toStartOfInterval form for our 4 fixed grains.
// Returned fragment expects a `timestamp` column in scope.
func DisplayGrainSQL(windowMs int64) string {
	switch displayGrain(windowMs) {
	case time.Minute:
		return "toStartOfMinute(timestamp)"
	case 5 * time.Minute:
		return "toStartOfFiveMinutes(timestamp)"
	case time.Hour:
		return "toStartOfHour(timestamp)"
	default:
		return "toStartOfDay(timestamp)"
	}
}

// WithBucketGrainSec appends `@bucketGrainSec` — the display-bucket grain
// in seconds (60 / 300 / 3600 / 86400 matching `DisplayGrain`). SQL emits
// per-display-bucket counts and divides by this to get a per-second rate
// within each bucket.
func WithBucketGrainSec(args []any, startMs, endMs int64) []any {
	sec := int64(displayGrain(endMs - startMs).Seconds())
	if sec <= 0 {
		sec = 60
	}
	return append(args, clickhouse.Named("bucketGrainSec", sec))
}
