package interfaces

import (
	"context"

	"github.com/observability/observability-backend-go/modules/ingestion/model"
)

// Repository defines the data access layer for telemetry records.
type Repository interface {
	InsertSpans(ctx context.Context, spans []model.SpanRecord) error
	InsertMetrics(ctx context.Context, metrics []model.MetricRecord) error
	InsertLogs(ctx context.Context, logs []model.LogRecord) error
}
