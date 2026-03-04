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
