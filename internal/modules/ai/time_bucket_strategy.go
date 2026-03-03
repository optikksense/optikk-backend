package ai

// TimeBucketStrategy defines the interface for time bucketing strategies
// Following Strategy Pattern and Open/Closed Principle - open for extension, closed for modification
type TimeBucketStrategy interface {
	GetBucketExpression() string
	GetBucketName() string
}

// AdaptiveTimeBucketStrategy selects the appropriate bucketing based on time range
// Following Strategy Pattern - encapsulates time bucketing algorithm
type AdaptiveTimeBucketStrategy struct {
	startMs int64
	endMs   int64
}

// NewAdaptiveTimeBucketStrategy creates a new adaptive time bucket strategy
func NewAdaptiveTimeBucketStrategy(startMs, endMs int64) *AdaptiveTimeBucketStrategy {
	return &AdaptiveTimeBucketStrategy{
		startMs: startMs,
		endMs:   endMs,
	}
}

// GetBucketExpression returns the SQL expression for time bucketing
func (s *AdaptiveTimeBucketStrategy) GetBucketExpression() string {
	hours := (s.endMs - s.startMs) / 3_600_000
	
	switch {
	case hours <= 3:
		return "DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00')"
	case hours <= 24:
		return "toStartOfFiveMinutes(timestamp)"
	case hours <= 168:
		return "toStartOfHour(timestamp)"
	default:
		return "toStartOfDay(timestamp)"
	}
}

// GetBucketName returns a human-readable name for the bucket size
func (s *AdaptiveTimeBucketStrategy) GetBucketName() string {
	hours := (s.endMs - s.startMs) / 3_600_000
	
	switch {
	case hours <= 3:
		return "1 minute"
	case hours <= 24:
		return "5 minutes"
	case hours <= 168:
		return "1 hour"
	default:
		return "1 day"
	}
}

// MinuteBucketStrategy buckets data by minute
type MinuteBucketStrategy struct{}

// NewMinuteBucketStrategy creates a new minute bucket strategy
func NewMinuteBucketStrategy() *MinuteBucketStrategy {
	return &MinuteBucketStrategy{}
}

// GetBucketExpression returns the SQL expression for minute bucketing
func (s *MinuteBucketStrategy) GetBucketExpression() string {
	return "DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00')"
}

// GetBucketName returns the bucket name
func (s *MinuteBucketStrategy) GetBucketName() string {
	return "1 minute"
}

// FiveMinuteBucketStrategy buckets data by 5 minutes
type FiveMinuteBucketStrategy struct{}

// NewFiveMinuteBucketStrategy creates a new 5-minute bucket strategy
func NewFiveMinuteBucketStrategy() *FiveMinuteBucketStrategy {
	return &FiveMinuteBucketStrategy{}
}

// GetBucketExpression returns the SQL expression for 5-minute bucketing
func (s *FiveMinuteBucketStrategy) GetBucketExpression() string {
	return "toStartOfFiveMinutes(timestamp)"
}

// GetBucketName returns the bucket name
func (s *FiveMinuteBucketStrategy) GetBucketName() string {
	return "5 minutes"
}

// HourBucketStrategy buckets data by hour
type HourBucketStrategy struct{}

// NewHourBucketStrategy creates a new hour bucket strategy
func NewHourBucketStrategy() *HourBucketStrategy {
	return &HourBucketStrategy{}
}

// GetBucketExpression returns the SQL expression for hour bucketing
func (s *HourBucketStrategy) GetBucketExpression() string {
	return "toStartOfHour(timestamp)"
}

// GetBucketName returns the bucket name
func (s *HourBucketStrategy) GetBucketName() string {
	return "1 hour"
}

// DayBucketStrategy buckets data by day
type DayBucketStrategy struct{}

// NewDayBucketStrategy creates a new day bucket strategy
func NewDayBucketStrategy() *DayBucketStrategy {
	return &DayBucketStrategy{}
}

// GetBucketExpression returns the SQL expression for day bucketing
func (s *DayBucketStrategy) GetBucketExpression() string {
	return "toStartOfDay(timestamp)"
}

// GetBucketName returns the bucket name
func (s *DayBucketStrategy) GetBucketName() string {
	return "1 day"
}

// TimeBucketStrategyFactory creates appropriate time bucket strategies
// Following Factory Pattern - encapsulates object creation logic
type TimeBucketStrategyFactory struct{}

// NewTimeBucketStrategyFactory creates a new factory
func NewTimeBucketStrategyFactory() *TimeBucketStrategyFactory {
	return &TimeBucketStrategyFactory{}
}

// CreateStrategy creates the appropriate strategy based on time range
func (f *TimeBucketStrategyFactory) CreateStrategy(startMs, endMs int64) TimeBucketStrategy {
	return NewAdaptiveTimeBucketStrategy(startMs, endMs)
}

// CreateStrategyByName creates a strategy by name
func (f *TimeBucketStrategyFactory) CreateStrategyByName(name string) TimeBucketStrategy {
	switch name {
	case "minute", "1m":
		return NewMinuteBucketStrategy()
	case "5minute", "5m":
		return NewFiveMinuteBucketStrategy()
	case "hour", "1h":
		return NewHourBucketStrategy()
	case "day", "1d":
		return NewDayBucketStrategy()
	default:
		return NewMinuteBucketStrategy()
	}
}

