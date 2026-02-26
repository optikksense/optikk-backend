package interfaces

import (
	"context"

	"github.com/observability/observability-backend-go/modules/spans/model"
)

// Repository defines the data access layer for traces.
type Repository interface {
	GetTraces(ctx context.Context, f model.TraceFilters, limit, offset int) ([]model.Trace, int64, model.TraceSummary, error)
	GetTraceSpans(ctx context.Context, teamUUID, traceID string) ([]model.Span, error)
	GetServiceDependencies(ctx context.Context, teamUUID string, startMs, endMs int64) ([]model.ServiceDependency, error)
	GetErrorGroups(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]model.ErrorGroup, error)
	GetErrorTimeSeries(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string) ([]model.ErrorTimeSeries, error)
	GetLatencyHistogram(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName, operationName string) ([]model.LatencyHistogramBucket, error)
	GetLatencyHeatmap(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string) ([]model.LatencyHeatmapPoint, error)
}
