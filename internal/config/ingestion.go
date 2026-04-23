package config

type IngestionConfig struct {
	SpansBucketSeconds int64         `yaml:"spans_bucket_seconds"`
	LogsBucketSeconds  int64         `yaml:"logs_bucket_seconds"`
	SpansIndexer       IndexerConfig `yaml:"spans_indexer"`
}

// IndexerConfig tunes the spans trace-assembly indexer. Defaults match
// internal/ingestion/spans/indexer.DefaultConfig(); override in config.yml
// for the production load profile (larger Capacity on high-volume tenants,
// tighter QuietWindowMs for faster "trace complete" emission when the
// collector mid-stream SDK produces spans in tight bursts).
type IndexerConfig struct {
	Capacity       int   `yaml:"capacity"`
	QuietWindowMs  int64 `yaml:"quiet_window_ms"`
	HardTimeoutMs  int64 `yaml:"hard_timeout_ms"`
	SweepEveryMs   int64 `yaml:"sweep_every_ms"`
}

func (c Config) SpansBucketSeconds() int64 {
	if c.Ingestion.SpansBucketSeconds <= 0 {
		return 300 // 5 minutes
	}
	return c.Ingestion.SpansBucketSeconds
}

// LogsBucketSeconds returns the ts_bucket_start granularity for observability.logs.
// Default is 1 day: log ingest volume dwarfs spans, and a coarser bucket keeps
// the partition-prune cost manageable at scale. Lower to 300 (match spans) in
// config when short-window dashboards dominate the read mix and log volume
// permits the finer clustering — note this only affects rows written after the
// change; historical rows retain their original bucket value.
func (c Config) LogsBucketSeconds() int64 {
	if c.Ingestion.LogsBucketSeconds <= 0 {
		return 86400 // 1 day
	}
	return c.Ingestion.LogsBucketSeconds
}

// SpansIndexerConfig returns the trace-assembly indexer tuning with defaults
// layered in (100k capacity, 10s quiet, 60s hard-timeout, 5s sweep). Callers
// pass the result to ingestion/spans/indexer.New.
func (c Config) SpansIndexerConfig() IndexerConfig {
	out := c.Ingestion.SpansIndexer
	if out.Capacity <= 0 {
		out.Capacity = 100_000
	}
	if out.QuietWindowMs <= 0 {
		out.QuietWindowMs = 10_000
	}
	if out.HardTimeoutMs <= 0 {
		out.HardTimeoutMs = 60_000
	}
	if out.SweepEveryMs <= 0 {
		out.SweepEveryMs = 5_000
	}
	return out
}
