package timebucket

// These bucket sizes mirror the ts_bucket_start partition keys used for
// ClickHouse PREWHERE pruning in spans and logs queries.
var (
	spansBucketSeconds int64 = 300
	logsBucketSeconds  int64 = 86400
)

const (
	millisecondsPerSecond = 1000
)

// Init sets the global bucket sizes from configuration.
func Init(spans, logs int64) {
	if spans > 0 {
		spansBucketSeconds = spans
	}
	if logs > 0 {
		logsBucketSeconds = logs
	}
}

func SpansBucketStart(unixSeconds int64) uint64 {
	return uint64(toBucketStart(unixSeconds, spansBucketSeconds))
}

func SpansBucketQueryBounds(startMs, endMs int64) (startBucket, endBucket uint64) {
	startSec := startMs / millisecondsPerSecond
	endSec := endMs / millisecondsPerSecond

	startBucket = SpansBucketStart(startSec)
	endBucket = SpansBucketStart(endSec)

	return startBucket, endBucket
}

func LogsBucketStart(unixSeconds int64) uint32 {
	return uint32(toBucketStart(unixSeconds, int64(logsBucketSeconds)))
}

func LogsBucketQueryBounds(startMs, endMs int64) (startBucket, endBucket uint32) {
	startSec := startMs / millisecondsPerSecond
	endSec := endMs / millisecondsPerSecond

	startBucket = LogsBucketStart(startSec)
	endBucket = LogsBucketStart(endSec)

	return startBucket, endBucket
}

func toBucketStart(unixSeconds int64, bucketSize int64) int64 {
	if bucketSize <= 0 {
		return unixSeconds
	}
	// Use integer division to truncate to the bucket boundary.
	return (unixSeconds / bucketSize) * bucketSize
}
