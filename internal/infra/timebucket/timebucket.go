// Package timebucket provides shared time-bucketing logic for ClickHouse queries.
// All metrics and observability modules should use this package instead of
// rolling their own time-bucket expression helpers.
package timebucket

import (
	"fmt"
)

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
