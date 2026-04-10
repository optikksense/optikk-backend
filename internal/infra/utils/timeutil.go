package utils

// Package utils provides shared time-bucketing logic for ClickHouse queries,
// as well as general time handling helpers.
// All metrics and observability modules should use this package instead of
// rolling their own time-bucket expression helpers.

import (
	"fmt"
	"time"
)

func ResolveRange(startPtr, endPtr *int64, defaultRangeMs int64) (start, end int64) {
	end = time.Now().UnixMilli()
	if endPtr != nil {
		end = *endPtr
	}
	start = end - defaultRangeMs
	if startPtr != nil {
		start = *startPtr
	}
	return start, end
}

func ParseMillis(v int64) time.Time {
	return time.UnixMilli(v).UTC()
}

type Strategy interface {
	GetBucketExpression() string
	GetBucketName() string
	GetRawExpression(column string) string
}

type MinuteStrategy struct{}

func (MinuteStrategy) GetBucketExpression() string {
	return MinuteStrategy{}.GetRawExpression("timestamp")
}
func (MinuteStrategy) GetRawExpression(column string) string {
	return fmt.Sprintf("formatDateTime(toStartOfMinute(%s), '%%Y-%%m-%%d %%H:%%i:00')", column)
}
func (MinuteStrategy) GetBucketName() string { return "1 minute" }

type FiveMinuteStrategy struct{}

func (FiveMinuteStrategy) GetBucketExpression() string {
	return FiveMinuteStrategy{}.GetRawExpression("timestamp")
}
func (FiveMinuteStrategy) GetRawExpression(column string) string {
	return fmt.Sprintf("formatDateTime(toStartOfFiveMinutes(%s), '%%Y-%%m-%%d %%H:%%i:00')", column)
}
func (FiveMinuteStrategy) GetBucketName() string { return "5 minutes" }

type FifteenMinuteStrategy struct{}

func (FifteenMinuteStrategy) GetBucketExpression() string {
	return FifteenMinuteStrategy{}.GetRawExpression("timestamp")
}
func (FifteenMinuteStrategy) GetRawExpression(column string) string {
	return fmt.Sprintf("formatDateTime(toStartOfFifteenMinutes(%s), '%%Y-%%m-%%d %%H:%%i:00')", column)
}
func (FifteenMinuteStrategy) GetBucketName() string { return "15 minutes" }

type HourStrategy struct{}

func (HourStrategy) GetBucketExpression() string {
	return HourStrategy{}.GetRawExpression("timestamp")
}
func (HourStrategy) GetRawExpression(column string) string {
	return fmt.Sprintf("formatDateTime(toStartOfHour(%s), '%%Y-%%m-%%d %%H:%%i:00')", column)
}
func (HourStrategy) GetBucketName() string { return "1 hour" }

type DayStrategy struct{}

func (DayStrategy) GetBucketExpression() string {
	return DayStrategy{}.GetRawExpression("timestamp")
}
func (DayStrategy) GetRawExpression(column string) string {
	return fmt.Sprintf("formatDateTime(toStartOfDay(%s), '%%Y-%%m-%%d %%H:%%i:00')", column)
}
func (DayStrategy) GetBucketName() string { return "1 day" }

type AdaptiveStrategy struct {
	startMs int64
	endMs   int64
}

func NewAdaptiveStrategy(startMs, endMs int64) *AdaptiveStrategy {
	return &AdaptiveStrategy{startMs: startMs, endMs: endMs}
}

func (s *AdaptiveStrategy) hours() int64 {
	return (s.endMs - s.startMs) / 3_600_000
}

func (s *AdaptiveStrategy) strategy() Strategy {
	h := s.hours()
	switch {
	case h <= 3:
		return MinuteStrategy{}
	case h <= 24:
		return FiveMinuteStrategy{}
	case h <= 168:
		return HourStrategy{}
	default:
		return DayStrategy{}
	}
}

func (s *AdaptiveStrategy) GetBucketExpression() string {
	return s.strategy().GetBucketExpression()
}

func (s *AdaptiveStrategy) GetRawExpression(column string) string {
	return s.strategy().GetRawExpression(column)
}

func (s *AdaptiveStrategy) GetBucketName() string {
	return s.strategy().GetBucketName()
}
func Expression(startMs, endMs int64) string {
	return NewAdaptiveStrategy(startMs, endMs).GetBucketExpression()
}

// ExprForColumn applies adaptive bucketing to a non-default timestamp column.
func ExprForColumn(startMs, endMs int64, column string) string {
	return NewAdaptiveStrategy(startMs, endMs).strategy().GetRawExpression(column)
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
		return MinuteStrategy{}
	case "5minute", "5m":
		return FiveMinuteStrategy{}
	case "15minute", "15m":
		return FifteenMinuteStrategy{}
	case "hour", "1h":
		return HourStrategy{}
	case "day", "1d":
		return DayStrategy{}
	default:
		return MinuteStrategy{}
	}
}

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
