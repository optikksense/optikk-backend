// Package timebucket is the single source of truth for every bucket value in the system. Writers and readers call the same helpers; ClickHouse never computes a bucket itself.
package timebucket

import "time"

const (
	SpansBucketSeconds int64 = 300
	LogsBucketSeconds  int64 = 86400
)

func SpansBucketStart(unixSeconds int64) uint64 {
	return uint64(toBucketStart(unixSeconds, SpansBucketSeconds))
}

func LogsBucketStart(unixSeconds int64) uint32 {
	return uint32(toBucketStart(unixSeconds, LogsBucketSeconds))
}

func toBucketStart(unixSeconds int64, bucketSize int64) int64 {
	return (unixSeconds / bucketSize) * bucketSize
}

func MetricsHourBucket(unixSeconds int64) time.Time {
	return time.Unix(unixSeconds, 0).UTC().Truncate(time.Hour)
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
