package interfaces

import (
	"context"

	"github.com/observability/observability-backend-go/modules/spans/model"
)

// Service defines the business logic layer for traces.
type Service interface {
	GetTraces(ctx context.Context, teamUUID string, startMs, endMs int64, limit, offset int, filters model.TraceFilters) (model.TraceSearchResponse, error)
	GetTraceSpans(ctx context.Context, teamUUID, traceID string) ([]model.Span, error)
	GetSpanTree(ctx context.Context, teamUUID, spanID string) ([]model.Span, error)
	GetServiceDependencies(ctx context.Context, teamUUID string, startMs, endMs int64) ([]model.ServiceDependency, error)
	GetErrorGroups(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]model.ErrorGroup, error)
	GetErrorTimeSeries(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string) ([]model.ErrorTimeSeries, error)
	GetLatencyHistogram(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName, operationName string) ([]model.LatencyHistogramBucket, error)
	GetLatencyHeatmap(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string) ([]model.LatencyHeatmapPoint, error)
}
