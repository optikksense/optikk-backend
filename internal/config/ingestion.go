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

func (c Config) LogsBucketSeconds() int64 {
	if c.Ingestion.LogsBucketSeconds <= 0 {
		return 86400 // 1 day
	}
	return c.Ingestion.LogsBucketSeconds
}
