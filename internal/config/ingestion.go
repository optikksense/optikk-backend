package config

type IngestionConfig struct {
	// ts_bucket grain is fixed in code (timebucket.SpansBucketSeconds /
	// timebucket.LogsBucketSeconds). Making it config-tunable was a footgun:
	// changing it post-deploy silently breaks reads against pre-change data
	// because writer- and reader-computed buckets stop matching. Changing the
	// grain now is a breaking schema change requiring a table rebuild.
	SpansIndexer IndexerConfig `yaml:"spans_indexer"`

	// Pipeline tunes the generic Kafka → Accumulator → Writer pipeline per
	// signal. Any field left zero inherits the defaults from
	// DefaultIngestPipelineConfig (tuned for the 200k records/s/instance
	// target). Overrides land in config.yml under
	// `ingestion.pipeline.{logs,spans,metrics}`.
	Pipeline IngestPipelinesConfig `yaml:"pipeline"`
}

// IngestPipelinesConfig holds one IngestPipelineConfig per signal. Keeping
// the sub-fields explicit (rather than a map) lets YAML validation catch
// typos in signal names and lets IDEs autocomplete the struct.
type IngestPipelinesConfig struct {
	Logs    IngestPipelineConfig `yaml:"logs"`
	Spans   IngestPipelineConfig `yaml:"spans"`
	Metrics IngestPipelineConfig `yaml:"metrics"`
}

// IngestPipelineConfig tunes accumulator triggers, worker queue, backpressure
// thresholds, writer retry schedule, and the CH async_insert setting for one
// signal. Zero values inherit the defaults.
type IngestPipelineConfig struct {
	MaxRows                int     `yaml:"max_rows"`
	MaxBytes               int     `yaml:"max_bytes"`
	MaxAgeMs               int64   `yaml:"max_age_ms"`
	WorkerQueueSize        int     `yaml:"worker_queue_size"`
	PauseDepthRatio        float64 `yaml:"pause_depth_ratio"`
	ResumeDepthRatio       float64 `yaml:"resume_depth_ratio"`
	WriterMaxAttempts      int     `yaml:"writer_max_attempts"`
	WriterBaseBackoffMs    int64   `yaml:"writer_base_backoff_ms"`
	WriterMaxBackoffMs     int64   `yaml:"writer_max_backoff_ms"`
	WriterAttemptTimeoutMs int64   `yaml:"writer_attempt_timeout_ms"`
	// AsyncInsert toggles CH server-side batching via SETTINGS async_insert=1.
	// Stored as a pointer so an explicit `false` in YAML can disable the
	// default-on behavior. Nil → default (true).
	AsyncInsert *bool `yaml:"async_insert"`
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

// DefaultIngestPipelineConfig returns the defaults used when a signal has no
// explicit override. Tuned for ≥200k records/s/instance: larger batches for
// fewer PrepareBatch round-trips, larger worker queue to absorb bursts,
// tighter age so p99 latency stays sub-second, async_insert on so CH batches
// across connections.
func DefaultIngestPipelineConfig() IngestPipelineConfig {
	t := true
	return IngestPipelineConfig{
		MaxRows:                10_000,
		MaxBytes:               16 * 1024 * 1024,
		MaxAgeMs:               250,
		WorkerQueueSize:        4096,
		PauseDepthRatio:        0.8,
		ResumeDepthRatio:       0.4,
		WriterMaxAttempts:      5,
		WriterBaseBackoffMs:    100,
		WriterMaxBackoffMs:     5_000,
		WriterAttemptTimeoutMs: 30_000,
		AsyncInsert:            &t,
	}
}

// IngestPipeline returns the pipeline tuning for the named signal with
// defaults layered in for any zero-valued field. Signal ∈ {"logs","spans","metrics"};
// an unknown signal returns the defaults unchanged.
func (c Config) IngestPipeline(signal string) IngestPipelineConfig {
	var raw IngestPipelineConfig
	switch signal {
	case "logs":
		raw = c.Ingestion.Pipeline.Logs
	case "spans":
		raw = c.Ingestion.Pipeline.Spans
	case "metrics":
		raw = c.Ingestion.Pipeline.Metrics
	}
	return mergeIngestPipeline(raw, DefaultIngestPipelineConfig())
}

// mergeIngestPipeline returns src with any zero-valued numeric field replaced
// by the same field from def, and a nil AsyncInsert replaced by def's pointer.
func mergeIngestPipeline(src, def IngestPipelineConfig) IngestPipelineConfig {
	if src.MaxRows <= 0 {
		src.MaxRows = def.MaxRows
	}
	if src.MaxBytes <= 0 {
		src.MaxBytes = def.MaxBytes
	}
	if src.MaxAgeMs <= 0 {
		src.MaxAgeMs = def.MaxAgeMs
	}
	if src.WorkerQueueSize <= 0 {
		src.WorkerQueueSize = def.WorkerQueueSize
	}
	if src.PauseDepthRatio <= 0 {
		src.PauseDepthRatio = def.PauseDepthRatio
	}
	if src.ResumeDepthRatio <= 0 {
		src.ResumeDepthRatio = def.ResumeDepthRatio
	}
	if src.WriterMaxAttempts <= 0 {
		src.WriterMaxAttempts = def.WriterMaxAttempts
	}
	if src.WriterBaseBackoffMs <= 0 {
		src.WriterBaseBackoffMs = def.WriterBaseBackoffMs
	}
	if src.WriterMaxBackoffMs <= 0 {
		src.WriterMaxBackoffMs = def.WriterMaxBackoffMs
	}
	if src.WriterAttemptTimeoutMs <= 0 {
		src.WriterAttemptTimeoutMs = def.WriterAttemptTimeoutMs
	}
	if src.AsyncInsert == nil {
		src.AsyncInsert = def.AsyncInsert
	}
	return src
}
