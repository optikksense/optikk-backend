package ingest

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/config"
)

// AccumulatorCfgFromPipeline projects an IngestPipelineConfig into the
// AccumulatorConfig shape. Keeps per-signal wiring free of YAML unit
// conversion.
func AccumulatorCfgFromPipeline(pc config.IngestPipelineConfig) AccumulatorConfig {
	return AccumulatorConfig{
		MaxRows:  pc.MaxRows,
		MaxBytes: pc.MaxBytes,
		MaxAge:   time.Duration(pc.MaxAgeMs) * time.Millisecond,
	}
}

// WriterCfgFromPipeline projects an IngestPipelineConfig into the
// WriterConfig shape (retry schedule + per-attempt timeout).
func WriterCfgFromPipeline(pc config.IngestPipelineConfig) WriterConfig {
	return WriterConfig{
		MaxAttempts: pc.WriterMaxAttempts,
		BaseBackoff: time.Duration(pc.WriterBaseBackoffMs) * time.Millisecond,
		MaxBackoff:  time.Duration(pc.WriterMaxBackoffMs) * time.Millisecond,
		Timeout:     time.Duration(pc.WriterAttemptTimeoutMs) * time.Millisecond,
	}
}

// DispatcherOptsFromPipeline derives Pause/Resume thresholds from the ratios
// on IngestPipelineConfig applied to WorkerQueueSize. Falls back to the
// package default when a ratio resolves to zero.
func DispatcherOptsFromPipeline(pc config.IngestPipelineConfig) DispatcherOptions {
	if pc.WorkerQueueSize <= 0 {
		return DefaultDispatcherOptions()
	}
	return DispatcherOptions{
		PauseDepth:  int(float64(pc.WorkerQueueSize) * pc.PauseDepthRatio),
		ResumeDepth: int(float64(pc.WorkerQueueSize) * pc.ResumeDepthRatio),
	}
}

// WithAsyncInsert returns a context-decorator that applies the CH
// `async_insert` / `wait_for_async_insert` settings when the pipeline config
// opts in. Applied per-attempt inside the Writer's send closure so retries
// use consistent settings. The `wait_for_async_insert=1` knob preserves
// at-least-once: INSERT still blocks until CH acks the buffer flush, so the
// retry path remains correct.
func WithIngestSettings(pc config.IngestPipelineConfig) func(context.Context) context.Context {
	settings := clickhouse.Settings{
		"max_partitions_per_insert_block": uint64(pc.MaxPartitionsPerInsertBlock),
	}
	if pc.AsyncInsert != nil && *pc.AsyncInsert {
		settings["async_insert"] = uint8(1)
		settings["wait_for_async_insert"] = uint8(1)
	}
	return func(ctx context.Context) context.Context {
		return clickhouse.Context(ctx, clickhouse.WithSettings(settings))
	}
}
