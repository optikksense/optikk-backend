package config

type IngestionConfig struct {
	SpansBucketSeconds int64 `yaml:"spans_bucket_seconds"`
	LogsBucketSeconds  int64 `yaml:"logs_bucket_seconds"`
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
