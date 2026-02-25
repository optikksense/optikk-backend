package telemetry

import "context"

// Ingester abstracts telemetry record ingestion.
// Implementations: DirectIngester (sync ClickHouse), KafkaIngester (Kafka producer).
type Ingester interface {
	IngestSpans(ctx context.Context, spans []SpanRecord) error
	IngestMetrics(ctx context.Context, metrics []MetricRecord) error
	IngestLogs(ctx context.Context, logs []LogRecord) error
	Close() error
}
