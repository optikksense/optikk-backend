package timebucket

// These bucket sizes mirror the ts_bucket_start partition keys used for
// ClickHouse PREWHERE pruning in spans and logs queries.
const (
	SpansBucketSeconds = 300

	LogsBucketSeconds = 86400

	millisecondsPerSecond = 1000
)

func SpansBucketStart(unixSeconds int64) uint64 {
	return uint64(unixSeconds / SpansBucketSeconds * SpansBucketSeconds) //nolint:gosec // G115 - domain-constrained value
}

func SpansBucketQueryBounds(startMs, endMs int64) (startBucket, endBucket uint64) {
	startSec := startMs / millisecondsPerSecond
	endSec := endMs / millisecondsPerSecond

	startBucket = SpansBucketStart(startSec)
	endBucket = SpansBucketStart(endSec)

	return startBucket, endBucket
}
func LogsBucketStart(unixSeconds int64) uint32 {
	return uint32(unixSeconds / LogsBucketSeconds * LogsBucketSeconds) //nolint:gosec // G115 - domain-constrained value
}

func LogsBucketQueryBounds(startMs, endMs int64) (startBucket, endBucket uint32) {
	startSec := startMs / millisecondsPerSecond
	endSec := endMs / millisecondsPerSecond

	startBucket = LogsBucketStart(startSec)
	endBucket = LogsBucketStart(endSec)

	return startBucket, endBucket
}
