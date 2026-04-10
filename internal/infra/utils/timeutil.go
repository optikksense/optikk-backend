package utils

// Package utils provides shared time-bucketing logic for ClickHouse queries.
// All metrics and observability modules should use this package instead of
// rolling their own time-bucket expression helpers.

import "fmt"

type Strategy interface {
	GetBucketExpression() string
	GetBucketName() string
	GetRawExpression(column string) string
}

type minuteStrategy struct{}

func (minuteStrategy) GetBucketExpression() string {
	return minuteStrategy{}.GetRawExpression("timestamp")
}
func (minuteStrategy) GetRawExpression(column string) string {
	return fmt.Sprintf("formatDateTime(toStartOfMinute(%s), '%%Y-%%m-%%d %%H:%%i:00')", column)
}
func (minuteStrategy) GetBucketName() string { return "1 minute" }

type fiveMinuteStrategy struct{}

func (fiveMinuteStrategy) GetBucketExpression() string {
	return fiveMinuteStrategy{}.GetRawExpression("timestamp")
}
func (fiveMinuteStrategy) GetRawExpression(column string) string {
	return fmt.Sprintf("formatDateTime(toStartOfFiveMinutes(%s), '%%Y-%%m-%%d %%H:%%i:00')", column)
}
func (fiveMinuteStrategy) GetBucketName() string { return "5 minutes" }

type fifteenMinuteStrategy struct{}

func (fifteenMinuteStrategy) GetBucketExpression() string {
	return fifteenMinuteStrategy{}.GetRawExpression("timestamp")
}
func (fifteenMinuteStrategy) GetRawExpression(column string) string {
	return fmt.Sprintf("formatDateTime(toStartOfFifteenMinutes(%s), '%%Y-%%m-%%d %%H:%%i:00')", column)
}
func (fifteenMinuteStrategy) GetBucketName() string { return "15 minutes" }

type hourStrategy struct{}

func (hourStrategy) GetBucketExpression() string {
	return hourStrategy{}.GetRawExpression("timestamp")
}
func (hourStrategy) GetRawExpression(column string) string {
	return fmt.Sprintf("formatDateTime(toStartOfHour(%s), '%%Y-%%m-%%d %%H:%%i:00')", column)
}
func (hourStrategy) GetBucketName() string { return "1 hour" }

type dayStrategy struct{}

func (dayStrategy) GetBucketExpression() string {
	return dayStrategy{}.GetRawExpression("timestamp")
}
func (dayStrategy) GetRawExpression(column string) string {
	return fmt.Sprintf("formatDateTime(toStartOfDay(%s), '%%Y-%%m-%%d %%H:%%i:00')", column)
}
func (dayStrategy) GetBucketName() string { return "1 day" }

func adaptiveStrategy(startMs, endMs int64) Strategy {
	h := (endMs - startMs) / 3_600_000
	switch {
	case h <= 3:
		return minuteStrategy{}
	case h <= 24:
		return fiveMinuteStrategy{}
	case h <= 168:
		return hourStrategy{}
	default:
		return dayStrategy{}
	}
}

func Expression(startMs, endMs int64) string {
	return adaptiveStrategy(startMs, endMs).GetBucketExpression()
}

// ExprForColumn applies adaptive bucketing to a non-default timestamp column.
func ExprForColumn(startMs, endMs int64, column string) string {
	return adaptiveStrategy(startMs, endMs).GetRawExpression(column)
}

// ExprForColumnTime applies adaptive bucketing while preserving a native
// ClickHouse DateTime value. Use this when the destination DTO scans into
// time.Time instead of a string timestamp.
func ExprForColumnTime(startMs, endMs int64, column string) string {
	h := (endMs - startMs) / 3_600_000
	switch {
	case h <= 3:
		return fmt.Sprintf("toStartOfMinute(%s)", column)
	case h <= 24:
		return fmt.Sprintf("toStartOfFiveMinutes(%s)", column)
	case h <= 168:
		return fmt.Sprintf("toStartOfHour(%s)", column)
	default:
		return fmt.Sprintf("toStartOfDay(%s)", column)
	}
}

func ByName(name string) Strategy {
	switch name {
	case "minute", "1m":
		return minuteStrategy{}
	case "5minute", "5m":
		return fiveMinuteStrategy{}
	case "15minute", "15m":
		return fifteenMinuteStrategy{}
	case "hour", "1h":
		return hourStrategy{}
	case "day", "1d":
		return dayStrategy{}
	default:
		return minuteStrategy{}
	}
}

// These bucket sizes mirror the ts_bucket_start partition keys used for
// ClickHouse PREWHERE pruning in spans and logs queries.
var (
	spansBucketSeconds int64 = 300
	logsBucketSeconds  int64 = 86400
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

func LogsBucketStart(unixSeconds int64) uint32 {
	return uint32(toBucketStart(unixSeconds, int64(logsBucketSeconds)))
}

func toBucketStart(unixSeconds int64, bucketSize int64) int64 {
	if bucketSize <= 0 {
		return unixSeconds
	}
	return (unixSeconds / bucketSize) * bucketSize
}
