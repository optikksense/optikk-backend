package ai

import timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"

// TimeBucketStrategy re-exports the shared interface so existing code in this
// package that depends on the type does not need to change import paths.
type TimeBucketStrategy = timebucket.Strategy

// NewAdaptiveTimeBucketStrategy is a convenience wrapper around the shared
// adaptive strategy.
func NewAdaptiveTimeBucketStrategy(startMs, endMs int64) TimeBucketStrategy {
	return timebucket.NewAdaptiveStrategy(startMs, endMs)
}

func NewMinuteBucketStrategy() TimeBucketStrategy     { return timebucket.MinuteStrategy{} }
func NewFiveMinuteBucketStrategy() TimeBucketStrategy { return timebucket.FiveMinuteStrategy{} }
func NewHourBucketStrategy() TimeBucketStrategy       { return timebucket.HourStrategy{} }
func NewDayBucketStrategy() TimeBucketStrategy        { return timebucket.DayStrategy{} }

// TimeBucketStrategyFactory creates appropriate time bucket strategies.
// Kept for backwards compatibility inside this package.
type TimeBucketStrategyFactory struct{}

func NewTimeBucketStrategyFactory() *TimeBucketStrategyFactory {
	return &TimeBucketStrategyFactory{}
}

func (f *TimeBucketStrategyFactory) CreateStrategy(startMs, endMs int64) TimeBucketStrategy {
	return timebucket.NewAdaptiveStrategy(startMs, endMs)
}

func (f *TimeBucketStrategyFactory) CreateStrategyByName(name string) TimeBucketStrategy {
	return timebucket.ByName(name)
}
