// Package timebucket is the single source of truth for every bucket value in the system. Writers and readers call the same helpers; ClickHouse never computes a bucket itself.
package timebucket

import "time"

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
