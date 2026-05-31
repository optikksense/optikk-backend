package consumer

import "time"

// HTTP response DTOs.

// TopicRatePoint — consume rate per topic per time bucket.
type TopicRatePoint struct {
	Timestamp  string  `json:"timestamp"`
	Topic      string  `json:"topic"`
	RatePerSec float64 `json:"rate_per_sec"`
}

// LagPoint — consumer lag per group+topic per time bucket.
type LagPoint struct {
	Timestamp     string  `json:"timestamp"`
	ConsumerGroup string  `json:"consumer_group"`
	Topic         string  `json:"topic"`
	Lag           float64 `json:"lag"`
}

// Internal repository row types.

type TopicCounterRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Topic     string    `ch:"topic"`
	Value     float64   `ch:"value"`
}

type GroupTopicGaugeRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	ConsumerGroup string    `ch:"consumer_group"`
	Topic         string    `ch:"topic"`
	Value         float64   `ch:"value"`
}
