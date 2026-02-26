package interfaces

import (
	"context"
	"time"

	"github.com/observability/observability-backend-go/modules/metrics/model"
)

// Service defines the business logic layer for metrics.
type Service interface {
	GetDashboardOverview(ctx context.Context, teamUUID string) (model.DashboardOverview, error)
	GetDashboardServices(ctx context.Context, teamUUID string) ([]model.ServiceHealth, error)
	GetDashboardServiceDetail(ctx context.Context, teamUUID, serviceName string) (model.ServiceHealth, error)
	GetServiceMetrics(ctx context.Context, teamUUID string, start, end time.Time) ([]model.ServiceMetric, error)
	GetEndpointMetrics(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.EndpointMetric, error)
	GetMetricsTimeSeries(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.TimeSeriesPoint, error)
	GetMetricsSummary(ctx context.Context, teamUUID string, start, end time.Time) (model.MetricsSummary, error)
	GetServiceTimeSeries(ctx context.Context, teamUUID string, start, end time.Time) ([]model.TimeSeriesPoint, error)
	GetEndpointTimeSeries(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.TimeSeriesPoint, error)
	GetServiceTopology(ctx context.Context, teamUUID string, start, end time.Time) (model.TopologyData, error)
	GetSystemStatus(ctx context.Context) map[string]any
}
