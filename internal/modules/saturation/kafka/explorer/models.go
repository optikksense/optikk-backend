package explorer

type KafkaSummaryResponse struct {
	TopicCount         int     `json:"topic_count"`
	GroupCount         int     `json:"group_count"`
	BytesPerSec        float64 `json:"bytes_per_sec"`
	AssignedPartitions float64 `json:"assigned_partitions"`
}

type KafkaTopicRow struct {
	Topic              string  `json:"topic"`
	BytesPerSec        float64 `json:"bytes_per_sec"`
	BytesTotal         float64 `json:"bytes_total"`
	RecordsPerSec      float64 `json:"records_per_sec"`
	RecordsTotal       float64 `json:"records_total"`
	Lag                float64 `json:"lag"`
	Lead               float64 `json:"lead"`
	ConsumerGroupCount int     `json:"consumer_group_count"`
}

type KafkaGroupRow struct {
	ConsumerGroup          string  `json:"consumer_group"`
	AssignedPartitions     float64 `json:"assigned_partitions"`
	CommitRate             float64 `json:"commit_rate"`
	CommitLatencyAvgMs     float64 `json:"commit_latency_avg_ms"`
	CommitLatencyMaxMs     float64 `json:"commit_latency_max_ms"`
	FetchRate              float64 `json:"fetch_rate"`
	FetchLatencyAvgMs      float64 `json:"fetch_latency_avg_ms"`
	FetchLatencyMaxMs      float64 `json:"fetch_latency_max_ms"`
	HeartbeatRate          float64 `json:"heartbeat_rate"`
	FailedRebalancePerHour float64 `json:"failed_rebalance_per_hour"`
	PollIdleRatio          float64 `json:"poll_idle_ratio"`
	LastPollSecondsAgo     float64 `json:"last_poll_seconds_ago"`
	ConnectionCount        float64 `json:"connection_count"`
	TopicCount             int     `json:"topic_count"`
}

type KafkaTopicConsumerRow struct {
	ConsumerGroup string  `json:"consumer_group"`
	BytesPerSec   float64 `json:"bytes_per_sec"`
	RecordsPerSec float64 `json:"records_per_sec"`
	Lag           float64 `json:"lag"`
	Lead          float64 `json:"lead"`
}

type KafkaTopicTrendPoint struct {
	Timestamp     string  `json:"timestamp"`
	BytesPerSec   float64 `json:"bytes_per_sec"`
	RecordsPerSec float64 `json:"records_per_sec"`
	Lag           float64 `json:"lag"`
	Lead          float64 `json:"lead"`
}

type KafkaTopicOverview struct {
	Topic   string                 `json:"topic"`
	Summary KafkaTopicRow          `json:"summary"`
	Trend   []KafkaTopicTrendPoint `json:"trend"`
}

type KafkaGroupTrendPoint struct {
	Timestamp              string  `json:"timestamp"`
	AssignedPartitions     float64 `json:"assigned_partitions"`
	CommitRate             float64 `json:"commit_rate"`
	FetchRate              float64 `json:"fetch_rate"`
	HeartbeatRate          float64 `json:"heartbeat_rate"`
	FailedRebalancePerHour float64 `json:"failed_rebalance_per_hour"`
	ConnectionCount        float64 `json:"connection_count"`
	PollIdleRatio          float64 `json:"poll_idle_ratio"`
	LastPollSecondsAgo     float64 `json:"last_poll_seconds_ago"`
}

type KafkaGroupOverview struct {
	ConsumerGroup string                 `json:"consumer_group"`
	Summary       KafkaGroupRow          `json:"summary"`
	Trend         []KafkaGroupTrendPoint `json:"trend"`
}

type KafkaGroupTopicRow struct {
	Topic         string  `json:"topic"`
	BytesPerSec   float64 `json:"bytes_per_sec"`
	BytesTotal    float64 `json:"bytes_total"`
	RecordsPerSec float64 `json:"records_per_sec"`
	RecordsTotal  float64 `json:"records_total"`
	Lag           float64 `json:"lag"`
	Lead          float64 `json:"lead"`
}

// ConsumerMetricSample — latest value per (consumer_group, node_id, metric_name)
// inside the query window. argMax(value, timestamp) collapses each time series
// to a single representative point.
type ConsumerMetricSample struct {
	Timestamp     string  `ch:"timestamp"     json:"timestamp"`
	ConsumerGroup string  `ch:"consumer_group" json:"consumer_group"`
	NodeID        string  `ch:"node_id"        json:"node_id"`
	MetricName    string  `ch:"metric_name"    json:"metric_name"`
	Value         float64 `ch:"value"          json:"value"`
}

// TopicMetricSample — latest value per (topic, consumer_group, metric_name)
// inside the query window.
type TopicMetricSample struct {
	Timestamp     string  `ch:"timestamp"     json:"timestamp"`
	Topic         string  `ch:"topic"          json:"topic"`
	ConsumerGroup string  `ch:"consumer_group" json:"consumer_group"`
	MetricName    string  `ch:"metric_name"    json:"metric_name"`
	Value         float64 `ch:"value"          json:"value"`
}
