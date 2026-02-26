package ingest

import (
	"context"

	"github.com/observability/observability-backend-go/internal/telemetry/model"
	"github.com/observability/observability-backend-go/internal/telemetry/store"
)

// DirectIngester writes telemetry records synchronously to ClickHouse
// via the Repository. This is the default mode when Kafka is disabled.
type DirectIngester struct {
	repo store.Repository
}

func NewDirectIngester(repo store.Repository) *DirectIngester {
	return &DirectIngester{repo: repo}
}

func (d *DirectIngester) IngestSpans(ctx context.Context, spans []model.SpanRecord) error {
	return d.repo.InsertSpans(ctx, spans)
}

func (d *DirectIngester) IngestMetrics(ctx context.Context, metrics []model.MetricRecord) error {
	return d.repo.InsertMetrics(ctx, metrics)
}

func (d *DirectIngester) IngestLogs(ctx context.Context, logs []model.LogRecord) error {
	return d.repo.InsertLogs(ctx, logs)
}

func (d *DirectIngester) Close() error {
	return nil
}
