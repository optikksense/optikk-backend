package kafka

// KafkaQueueLag represents the raw lag per queue.
type KafkaQueueLag struct {
	Timestamp      string  `json:"timestamp"`
	Queue          string  `json:"queue"`
	AvgConsumerLag float64 `json:"avg_consumer_lag"`
	MaxConsumerLag float64 `json:"max_consumer_lag"`
}

// KafkaProductionRate represents messages produced per second.
type KafkaProductionRate struct {
	Timestamp      string  `json:"timestamp"`
	Topic          string  `json:"topic"`
	AvgPublishRate float64 `json:"avg_publish_rate"`
}

// KafkaConsumptionRate represents messages consumed per second.
type KafkaConsumptionRate struct {
	Timestamp      string  `json:"timestamp"`
	Topic          string  `json:"topic"`
	AvgReceiveRate float64 `json:"avg_receive_rate"`
}

// MqBucket represents consumer lag timeseries or topic lag timeseries for kafka.
type MqBucket struct {
	Timestamp       string  `json:"timestamp"` // 1 minute buckets
	ServiceName     string  `json:"service"`
	QueueName       string  `json:"queue"`
	MessagingSystem string  `json:"messaging_system"`
	AvgQueueDepth   float64 `json:"avg_queue_depth"`
	AvgConsumerLag  float64 `json:"avg_consumer_lag"`
}

// MqTopQueue represents the top kafka queues by message rates.
type MqTopQueue struct {
	ServiceName       string  `json:"service_name"`
	QueueName         string  `json:"queue_name"`
	MessagingSystem   string  `json:"messaging_system"`
	AvgPublishRate    float64 `json:"avg_publish_rate"`
	AvgReceiveRate    float64 `json:"avg_receive_rate"`
	AvgQueueDepth     float64 `json:"avg_queue_depth"`
	AvgConsumerLag    float64 `json:"avg_consumer_lag"`
	MaxConsumerLag    float64 `json:"max_consumer_lag"`
	ActiveConnections int64   `json:"active_connections"`
	SampleCount       int64   `json:"sample_count"`
	PublishRetry      float64 `json:"publish_retry"`
}

// PartitionLag represents consumer lag per topic/partition/consumer group.
type PartitionLag struct {
	Topic         string `json:"topic"`
	Partition     int64  `json:"partition"`
	ConsumerGroup string `json:"consumer_group"`
	Lag           int64  `json:"lag"`
}

// MessageRates holds published and consumed message rates.
type MessageRates struct {
	PublishedPerSec float64 `json:"published_per_sec"`
	ConsumedPerSec  float64 `json:"consumed_per_sec"`
}

// HistogramSummary holds p50/p95/p99/avg for histogram metrics.
type HistogramSummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Avg float64 `json:"avg"`
}

// OffsetTimeSeries represents consumer offset over time per topic.
type OffsetTimeSeries struct {
	Timestamp string   `json:"timestamp"`
	Topic     string   `json:"topic"`
	Value     *float64 `json:"value"`
}
