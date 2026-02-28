package telemetry

import (
	"context"
	"time"
)

// Ingester abstracts telemetry record ingestion.
// DirectIngester buffers asynchronously; KafkaIngester produces to Kafka.
type Ingester interface {
	IngestSpans(ctx context.Context, spans []SpanRecord) error
	IngestMetrics(ctx context.Context, metrics []MetricRecord) error
	IngestLogs(ctx context.Context, logs []LogRecord) error
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
		spans:   newAsyncBuffer(cfg.SpansBatchSize, cfg.FlushInterval, repo.InsertSpans),
		metrics: newAsyncBuffer(cfg.MetricsBatchSize, cfg.FlushInterval, repo.InsertMetrics),
		logs:    newAsyncBuffer(cfg.LogsBatchSize, cfg.FlushInterval, repo.InsertLogs),
	}
}

func (d *DirectIngester) IngestSpans(_ context.Context, spans []SpanRecord) error {
	d.spans.Append(spans)
	return nil
}

func (d *DirectIngester) IngestMetrics(_ context.Context, metrics []MetricRecord) error {
	d.metrics.Append(metrics)
	return nil
}

func (d *DirectIngester) IngestLogs(_ context.Context, logs []LogRecord) error {
	d.logs.Append(logs)
	return nil
}

// Close flushes all remaining buffered records before returning.
func (d *DirectIngester) Close() error {
	d.spans.Close()
	d.metrics.Close()
	d.logs.Close()
	return nil
}
