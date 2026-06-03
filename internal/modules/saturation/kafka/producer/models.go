package producer

import "time"

// HTTP response DTOs.

// TopicRatePoint — produce rate per topic per time bucket.
type TopicRatePoint struct {
	Timestamp  string  `json:"timestamp"`
	Topic      string  `json:"topic"`
	RatePerSec float64 `json:"rate_per_sec"`
}

// Internal repository row types.

type TopicCounterRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Topic     string    `ch:"topic"`
	Value     float64   `ch:"value"`
}
