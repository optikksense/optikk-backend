package interfaces

import (
	"context"

	"github.com/observability/observability-backend-go/modules/ingestion/model"
)

// Ingester abstracts telemetry record ingestion.
// Implementations: DirectIngester (sync ClickHouse), KafkaIngester (Kafka producer).
type Ingester interface {
	IngestSpans(ctx context.Context, spans []model.SpanRecord) error
	IngestMetrics(ctx context.Context, metrics []model.MetricRecord) error
	IngestLogs(ctx context.Context, logs []model.LogRecord) error
	Close() error
}
