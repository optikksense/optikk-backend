package telemetry

import "context"

// DirectIngester writes telemetry records synchronously to ClickHouse
// via the Repository. This is the default mode when Kafka is disabled.
type DirectIngester struct {
	repo *Repository
}

func NewDirectIngester(repo *Repository) *DirectIngester {
	return &DirectIngester{repo: repo}
}

func (d *DirectIngester) IngestSpans(ctx context.Context, spans []SpanRecord) error {
	return d.repo.InsertSpans(ctx, spans)
}

func (d *DirectIngester) IngestMetrics(ctx context.Context, metrics []MetricRecord) error {
	return d.repo.InsertMetrics(ctx, metrics)
}

func (d *DirectIngester) IngestLogs(ctx context.Context, logs []LogRecord) error {
	return d.repo.InsertLogs(ctx, logs)
}

func (d *DirectIngester) Close() error {
	return nil
}
