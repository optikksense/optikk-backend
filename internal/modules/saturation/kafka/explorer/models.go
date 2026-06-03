package explorer

// Topic Domains
type TopicThroughputRow struct {
	Topic         string  `ch:"topic"                json:"topic"`
	BytesPerSec   float64 `ch:"bytes_per_sec"        json:"bytes_per_sec"`
	BytesTotal    float64 `ch:"bytes_total"           json:"bytes_total"`
	RecordsPerSec float64 `ch:"records_per_sec"      json:"records_per_sec"`
	RecordsTotal  float64 `ch:"records_total"         json:"records_total"`
}

type TopicLagRow struct {
	Topic string  `ch:"topic" json:"topic"`
	Lag   float64 `ch:"lag"   json:"lag"`
	Lead  float64 `ch:"lead"  json:"lead"`
}

type TopicConsumersRow struct {
	Topic              string `ch:"topic"                json:"topic"`
	ConsumerGroupCount uint64 `ch:"consumer_group_count" json:"consumer_group_count"`
}

// Consumer Group Domains
type GroupPartitionsRow struct {
	ConsumerGroup      string  `ch:"consumer_group"      json:"consumer_group"`
	AssignedPartitions float64 `ch:"assigned_partitions" json:"assigned_partitions"`
}

type GroupCommitsRow struct {
	ConsumerGroup      string  `ch:"consumer_group"        json:"consumer_group"`
	CommitRate         float64 `ch:"commit_rate"           json:"commit_rate"`
	CommitLatencyAvgMs float64 `ch:"commit_latency_avg_ms" json:"commit_latency_avg_ms"`
	CommitLatencyMaxMs float64 `ch:"commit_latency_max_ms" json:"commit_latency_max_ms"`
}

type GroupFetchesRow struct {
	ConsumerGroup     string  `ch:"consumer_group"       json:"consumer_group"`
	FetchRate         float64 `ch:"fetch_rate"           json:"fetch_rate"`
	FetchLatencyAvgMs float64 `ch:"fetch_latency_avg_ms" json:"fetch_latency_avg_ms"`
	FetchLatencyMaxMs float64 `ch:"fetch_latency_max_ms" json:"fetch_latency_max_ms"`
}

type GroupHealthRow struct {
	ConsumerGroup          string  `ch:"consumer_group"            json:"consumer_group"`
	HeartbeatRate          float64 `ch:"heartbeat_rate"            json:"heartbeat_rate"`
	FailedRebalancePerHour float64 `ch:"failed_rebalance_per_hour" json:"failed_rebalance_per_hour"`
	PollIdleRatio          float64 `ch:"poll_idle_ratio"           json:"poll_idle_ratio"`
	LastPollSecondsAgo     float64 `ch:"last_poll_seconds_ago"     json:"last_poll_seconds_ago"`
	ConnectionCount        float64 `ch:"connection_count"          json:"connection_count"`
}

// Legacy Trends and Detail (To be refactored or deleted in a real project, but for now we'll keep the trend points for the detail page charts if needed)
type KafkaTopicTrendPoint struct {
	Timestamp     string  `json:"timestamp"`
	BytesPerSec   float64 `json:"bytes_per_sec"`
	RecordsPerSec float64 `json:"records_per_sec"`
	Lag           float64 `json:"lag"`
	Lead          float64 `json:"lead"`
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
