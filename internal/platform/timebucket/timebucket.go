// Package timebucket provides shared time-bucketing logic for ClickHouse queries.
// All metrics and observability modules should use this package instead of
// rolling their own time-bucket expression helpers.
package timebucket

import (
	"fmt"
)

// Strategy defines the interface for time bucketing strategies.
type Strategy interface {
	GetBucketExpression() string
	GetBucketName() string
}

// ─────────────────────────────────────────────────────────────────────────────
// Concrete strategies
// ─────────────────────────────────────────────────────────────────────────────

// MinuteStrategy buckets data by 1 minute.
type MinuteStrategy struct{}

func (MinuteStrategy) GetBucketExpression() string {
	return "formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%d %H:%i:00')"
}
func (MinuteStrategy) GetBucketName() string { return "1 minute" }

// FiveMinuteStrategy buckets data by 5 minutes.
type FiveMinuteStrategy struct{}

func (FiveMinuteStrategy) GetBucketExpression() string {
	return "formatDateTime(toStartOfFiveMinutes(timestamp), '%Y-%m-%d %H:%i:00')"
}
func (FiveMinuteStrategy) GetBucketName() string { return "5 minutes" }

// HourStrategy buckets data by 1 hour.
type HourStrategy struct{}

func (HourStrategy) GetBucketExpression() string {
	return "formatDateTime(toStartOfHour(timestamp), '%Y-%m-%d %H:%i:00')"
}
func (HourStrategy) GetBucketName() string { return "1 hour" }

// DayStrategy buckets data by 1 day.
type DayStrategy struct{}

func (DayStrategy) GetBucketExpression() string {
	return "formatDateTime(toStartOfDay(timestamp), '%Y-%m-%d %H:%i:00')"
}
func (DayStrategy) GetBucketName() string { return "1 day" }

// ─────────────────────────────────────────────────────────────────────────────
// Adaptive (auto-selects granularity based on time range)
// ─────────────────────────────────────────────────────────────────────────────

// AdaptiveStrategy selects the appropriate granularity for a given time range.
type AdaptiveStrategy struct {
	startMs int64
	endMs   int64
}

// NewAdaptiveStrategy creates an AdaptiveStrategy for the given millisecond range.
func NewAdaptiveStrategy(startMs, endMs int64) *AdaptiveStrategy {
	return &AdaptiveStrategy{startMs: startMs, endMs: endMs}
}

func (s *AdaptiveStrategy) hours() int64 {
	return (s.endMs - s.startMs) / 3_600_000
}

// GetBucketExpression returns the ClickHouse SQL expression for time bucketing.
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

// GetBucketName returns the human-readable granularity label.
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

// ─────────────────────────────────────────────────────────────────────────────
// Convenience helpers
// ─────────────────────────────────────────────────────────────────────────────

// Expression is a convenience wrapper that returns the SQL expression for the
// given millisecond range without needing to instantiate a Strategy explicitly.
// It uses the standard `timestamp` column name.
func Expression(startMs, endMs int64) string {
	return NewAdaptiveStrategy(startMs, endMs).GetBucketExpression()
}

// ExprForColumn returns the adaptive bucket SQL expression using a custom
// column name. Useful for span tables that use `start_time` instead of `timestamp`.
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

// ByName returns a Strategy by short name. Useful when a caller wants to force
// a specific granularity regardless of the time range.
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
