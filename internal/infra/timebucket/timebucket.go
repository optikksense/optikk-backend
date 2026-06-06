// Package timebucket provides the source of truth for system bucket values.
// Both writers and readers call the same helpers to align bucket values.
package timebucket

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const BucketSeconds int64 = 300

// BucketStart truncates a Unix-second timestamp to its 5-minute bucket.
// Returned as UInt32 for lossless round-trips through ClickHouse.
func BucketStart(unixSeconds int64) uint32 {
	return uint32((unixSeconds / BucketSeconds) * BucketSeconds)
}

// BucketTime expands a ts_bucket UInt32 back into a UTC time.Time.
// Use this to scan ts_bucket natively instead of casting in SQL.
func BucketTime(b uint32) time.Time {
	return time.Unix(int64(b), 0).UTC()
}

// BucketDateTimeString formats a ts_bucket UInt32 as "YYYY-MM-DD HH:MM:SS".
// Used when API contracts expect a string representation.
func BucketDateTimeString(b uint32) string {
	return time.Unix(int64(b), 0).UTC().Format("2006-01-02 15:04:05")
}

// FormatDisplayBucket formats a display-grain bucket time.Time as UTC
// "YYYY-MM-DD HH:MM:SS" for consistency with ClickHouse string formats.
func FormatDisplayBucket(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}

// DisplayBucket buckets a row timestamp into a display-time bucket.
// Grain is based on query window: <=3h: 1m, <=24h: 5m, <=7d: 1h, else 1d.
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

// DisplayGrainSQL returns the fastest CH SQL fragment for adaptive grain.
// The returned fragment expects a `timestamp` column in scope.
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

// WithBucketGrainSec appends @bucketGrainSec in seconds matching DisplayGrain.
// Used to compute rates from counts within each display bucket.
func WithBucketGrainSec(args []any, startMs, endMs int64) []any {
	sec := int64(displayGrain(endMs - startMs).Seconds())
	if sec <= 0 {
		sec = 60
	}
	return append(args, clickhouse.Named("bucketGrainSec", sec))
}
