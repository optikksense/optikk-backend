package config

import "time"

type IngestionConfig struct {
	SpansBucketSeconds int64 `yaml:"spans_bucket_seconds"`
	LogsBucketSeconds  int64 `yaml:"logs_bucket_seconds"`
	// ByteTrackerFlushIntervalMs is the frequency at which byte tracking totals are flushed to MySQL.
	ByteTrackerFlushIntervalMs int64 `yaml:"byte_tracker_flush_interval_ms"`
	// BatchMaxRows is the maximum number of rows to buffer before flushing to ClickHouse.
	BatchMaxRows int `yaml:"batch_max_rows"`
	// BatchMaxWaitMs is the maximum time in milliseconds to wait before flushing a partial batch to ClickHouse.
	BatchMaxWaitMs int64 `yaml:"batch_max_wait_ms"`
}

func (c Config) SpansBucketSeconds() int64 {
	if c.Ingestion.SpansBucketSeconds <= 0 {
		return 300 // 5 minutes
	}
	return c.Ingestion.SpansBucketSeconds
}

func (c Config) LogsBucketSeconds() int64 {
	if c.Ingestion.LogsBucketSeconds <= 0 {
		return 86400 // 1 day
	}
	return c.Ingestion.LogsBucketSeconds
}

func (c Config) ByteTrackerFlushInterval() time.Duration {
	ms := c.Ingestion.ByteTrackerFlushIntervalMs
	if ms <= 0 {
		return 5 * time.Minute
	}
	return time.Duration(ms) * time.Millisecond
}

func (c Config) IngestionBatchMaxRows() int {
	n := c.Ingestion.BatchMaxRows
	if n <= 0 {
		return 5000
	}
	return n
}

func (c Config) IngestionBatchMaxWait() time.Duration {
	ms := c.Ingestion.BatchMaxWaitMs
	if ms <= 0 {
		return 5 * time.Second
	}
	return time.Duration(ms) * time.Millisecond
}
