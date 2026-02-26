package store

import (
	"context"
	"time"

	"github.com/observability/observability-backend-go/internal/modules/metrics/model"
)

// Repository defines the data access layer for metrics.
type Repository interface {
	GetDashboardOverview(ctx context.Context, teamUUID string, start, end time.Time) ([]map[string]any, []map[string]any, []map[string]any, error)
	GetDashboardServices(ctx context.Context, teamUUID string, start, end time.Time) ([]map[string]any, error)
	GetDashboardServiceDetail(ctx context.Context, teamUUID, serviceName string, start, end time.Time) (map[string]any, error)
	GetServiceMetrics(ctx context.Context, teamUUID string, start, end time.Time) ([]model.ServiceMetric, error)
	GetEndpointMetrics(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.EndpointMetric, error)
	GetMetricsTimeSeries(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.TimeSeriesPoint, error)
	GetMetricsSummary(ctx context.Context, teamUUID string, start, end time.Time) (model.MetricsSummary, error)
	GetServiceTimeSeries(ctx context.Context, teamUUID string, start, end time.Time) ([]model.TimeSeriesPoint, error)
	GetServiceTopologyNodes(ctx context.Context, teamUUID string, start, end time.Time) ([]model.TopologyNode, error)
	GetServiceTopologyEdges(ctx context.Context, teamUUID string, start, end time.Time) ([]model.TopologyEdge, error)
	GetEndpointTimeSeries(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.TimeSeriesPoint, error)
}
