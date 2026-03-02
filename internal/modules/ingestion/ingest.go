package telemetry

import (
	"context"
	"time"
)

// Ingester abstracts telemetry record ingestion.
// DirectIngester buffers asynchronously; KafkaIngester produces to Kafka.
// Methods return a *BatchInsertResult describing how many records were accepted
// vs rejected. For async ingesters (DirectIngester), all records are accepted
// into the buffer immediately and partial failures are handled during flush.
type Ingester interface {
	IngestSpans(ctx context.Context, spans []SpanRecord) (*BatchInsertResult, error)
	IngestMetrics(ctx context.Context, metrics []MetricRecord) (*BatchInsertResult, error)
	IngestLogs(ctx context.Context, logs []LogRecord) (*BatchInsertResult, error)
	Close() error
}

// DirectIngesterConfig controls the async buffer for direct-mode ingestion.
// Zero values use sensible defaults.
type DirectIngesterConfig struct {
	SpansBatchSize   int
	MetricsBatchSize int
	LogsBatchSize    int
	FlushInterval    time.Duration
}

// DirectIngester buffers telemetry records in memory and flushes them to
// ClickHouse asynchronously. IngestSpans/Metrics/Logs return immediately,
// making the HTTP handler non-blocking for telemetry data.
type DirectIngester struct {
	spans   *AsyncBuffer[SpanRecord]
	metrics *AsyncBuffer[MetricRecord]
	logs    *AsyncBuffer[LogRecord]
}

func NewDirectIngester(repo *Repository, cfg DirectIngesterConfig) *DirectIngester {
	if cfg.SpansBatchSize <= 0 {
		cfg.SpansBatchSize = 5000
	}
	if cfg.MetricsBatchSize <= 0 {
		cfg.MetricsBatchSize = 1000
	}
	if cfg.LogsBatchSize <= 0 {
		cfg.LogsBatchSize = 5000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	return &DirectIngester{
		spans: newAsyncBuffer(cfg.SpansBatchSize, cfg.FlushInterval, func(ctx context.Context, spans []SpanRecord) error {
			_, err := repo.InsertSpans(ctx, spans)
			return err
		}),
		metrics: newAsyncBuffer(cfg.MetricsBatchSize, cfg.FlushInterval, func(ctx context.Context, metrics []MetricRecord) error {
			_, err := repo.InsertMetrics(ctx, metrics)
			return err
		}),
		logs: newAsyncBuffer(cfg.LogsBatchSize, cfg.FlushInterval, func(ctx context.Context, logs []LogRecord) error {
			_, err := repo.InsertLogs(ctx, logs)
			return err
		}),
	}
}

func (d *DirectIngester) IngestSpans(_ context.Context, spans []SpanRecord) (*BatchInsertResult, error) {
	d.spans.Append(spans)
	return &BatchInsertResult{TotalCount: len(spans), AcceptedCount: len(spans)}, nil
}

func (d *DirectIngester) IngestMetrics(_ context.Context, metrics []MetricRecord) (*BatchInsertResult, error) {
	d.metrics.Append(metrics)
	return &BatchInsertResult{TotalCount: len(metrics), AcceptedCount: len(metrics)}, nil
}

func (d *DirectIngester) IngestLogs(_ context.Context, logs []LogRecord) (*BatchInsertResult, error) {
	d.logs.Append(logs)
	return &BatchInsertResult{TotalCount: len(logs), AcceptedCount: len(logs)}, nil
}

// Close flushes all remaining buffered records before returning.
func (d *DirectIngester) Close() error {
	d.spans.Close()
	d.metrics.Close()
	d.logs.Close()
	return nil
}
