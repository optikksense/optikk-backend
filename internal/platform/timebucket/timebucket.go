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
}
type MinuteStrategy struct{}

func (MinuteStrategy) GetBucketExpression() string {
	return "formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%d %H:%i:00')"
}
func (MinuteStrategy) GetBucketName() string { return "1 minute" }

type FiveMinuteStrategy struct{}

func (FiveMinuteStrategy) GetBucketExpression() string {
	return "formatDateTime(toStartOfFiveMinutes(timestamp), '%Y-%m-%d %H:%i:00')"
}
func (FiveMinuteStrategy) GetBucketName() string { return "5 minutes" }

type HourStrategy struct{}

func (HourStrategy) GetBucketExpression() string {
	return "formatDateTime(toStartOfHour(timestamp), '%Y-%m-%d %H:%i:00')"
}
func (HourStrategy) GetBucketName() string { return "1 hour" }

type DayStrategy struct{}

func (DayStrategy) GetBucketExpression() string {
	return "formatDateTime(toStartOfDay(timestamp), '%Y-%m-%d %H:%i:00')"
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

func (s *AdaptiveStrategy) GetBucketExpression() string {
	switch h := s.hours(); {
	case h <= 3:
		return MinuteStrategy{}.GetBucketExpression()
	case h <= 24:
		return FiveMinuteStrategy{}.GetBucketExpression()
	case h <= 168:
		return HourStrategy{}.GetBucketExpression()
	default:
		return DayStrategy{}.GetBucketExpression()
	}
}

func (s *AdaptiveStrategy) GetBucketName() string {
	switch h := s.hours(); {
	case h <= 3:
		return MinuteStrategy{}.GetBucketName()
	case h <= 24:
		return FiveMinuteStrategy{}.GetBucketName()
	case h <= 168:
		return HourStrategy{}.GetBucketName()
	default:
		return DayStrategy{}.GetBucketName()
	}
}
func Expression(startMs, endMs int64) string {
	return NewAdaptiveStrategy(startMs, endMs).GetBucketExpression()
}

// ExprForColumn applies adaptive bucketing to a non-default timestamp column.
func ExprForColumn(startMs, endMs int64, column string) string {
	h := (endMs - startMs) / 3_600_000
	switch {
	case h <= 3:
		return fmt.Sprintf("formatDateTime(toStartOfMinute(%s), '%%Y-%%m-%%d %%H:%%i:00')", column)
	case h <= 24:
		return fmt.Sprintf("formatDateTime(toStartOfFiveMinutes(%s), '%%Y-%%m-%%d %%H:%%i:00')", column)
	case h <= 168:
		return fmt.Sprintf("formatDateTime(toStartOfHour(%s), '%%Y-%%m-%%d %%H:%%i:00')", column)
	default:
		return fmt.Sprintf("formatDateTime(toStartOfDay(%s), '%%Y-%%m-%%d %%H:%%i:00')", column)
	}
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
	case "hour", "1h":
		return HourStrategy{}
	case "day", "1d":
		return DayStrategy{}
	default:
		return MinuteStrategy{}
	}
}
