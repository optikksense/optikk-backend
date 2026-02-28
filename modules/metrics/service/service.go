package service

import (
	"context"
	"strings"
	"time"

	"github.com/observability/observability-backend-go/internal/platform/handlers"
	"github.com/observability/observability-backend-go/modules/metrics/model"
	"github.com/observability/observability-backend-go/modules/metrics/store"
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

type MetricService struct {
	repo store.Repository
}

func NewService(repo store.Repository) *MetricService {
	return &MetricService{repo: repo}
}

func (s *MetricService) GetDashboardOverview(ctx context.Context, teamUUID string) (model.DashboardOverview, error) {
	end := time.Now().UTC()
	start := end.Add(-1 * time.Hour)

	serviceMetrics, logsData, tracesData, err := s.repo.GetDashboardOverview(ctx, teamUUID, start, end)
	if err != nil {
		return model.DashboardOverview{}, err
	}

	totalRequests := int64(0)
	totalLatency := 0.0
	for _, row := range serviceMetrics {
		totalRequests += handlers.Int64FromAny(row["request_count"])
		totalLatency += handlers.Float64FromAny(row["avg_latency"])
	}
	avgLatency := 0.0
	if len(serviceMetrics) > 0 {
		avgLatency = totalLatency / float64(len(serviceMetrics))
	}

	levelCounts := map[string]int64{}
	for _, logRow := range logsData {
		levelCounts[handlers.StringFromAny(logRow["level"])]++
	}

	errorTraces := int64(0)
	for _, t := range tracesData {
		if strings.EqualFold(handlers.StringFromAny(t["status"]), "ERROR") {
			errorTraces++
		}
	}
	statusCounts := map[string]int64{
		"OK":    int64(len(tracesData)) - errorTraces,
		"ERROR": errorTraces,
	}

	return model.DashboardOverview{
		Metrics: model.MetricOverview{
			Count:  len(serviceMetrics),
			Recent: []any{}, // TODO: populate if needed
			Statistics: model.MetricStats{
				AvgLatency:    avgLatency,
				TotalRequests: totalRequests,
			},
		},
		Logs: model.LogOverview{
			Count:       len(logsData),
			Recent:      []any{}, // TODO: populate if needed
			LevelCounts: levelCounts,
		},
		Traces: model.TraceOverview{
			Count:        len(tracesData),
			Recent:       []any{}, // TODO: populate if needed
			StatusCounts: statusCounts,
		},
		TimeRange: model.TimeRange{
			Start: start.UnixMilli(),
			End:   end.UnixMilli(),
		},
	}, nil
}

func (s *MetricService) GetDashboardServices(ctx context.Context, teamUUID string) ([]model.ServiceHealth, error) {
	end := time.Now().UTC()
	start := end.Add(-1 * time.Hour)
	rows, err := s.repo.GetDashboardServices(ctx, teamUUID, start, end)
	if err != nil {
		return nil, err
	}

	services := make([]model.ServiceHealth, 0, len(rows))
	for _, r := range rows {
		requestCount := handlers.Int64FromAny(r["request_count"])
		errorCount := handlers.Int64FromAny(r["error_count"])
		errorRate := 0.0
		if requestCount > 0 {
			errorRate = float64(errorCount) * 100.0 / float64(requestCount)
		}

		services = append(services, model.ServiceHealth{
			Name:        handlers.StringFromAny(r["service_name"]),
			Status:      s.getServiceStatus(errorRate),
			MetricCount: requestCount,
			LogCount:    0,
			TraceCount:  requestCount,
			ErrorCount:  errorCount,
			ErrorRate:   errorRate,
			LastSeen:    time.Now().UnixMilli(),
		})
	}
	return services, nil
}

func (s *MetricService) GetDashboardServiceDetail(ctx context.Context, teamUUID, serviceName string) (model.ServiceHealth, error) {
	end := time.Now().UTC()
	start := end.Add(-1 * time.Hour)
	row, err := s.repo.GetDashboardServiceDetail(ctx, teamUUID, serviceName, start, end)
	if err != nil {
		return model.ServiceHealth{}, err
	}

	if len(row) == 0 {
		return model.ServiceHealth{}, nil
	}

	requestCount := handlers.Int64FromAny(row["request_count"])
	errorCount := handlers.Int64FromAny(row["error_count"])
	errorRate := 0.0
	if requestCount > 0 {
		errorRate = float64(errorCount) * 100.0 / float64(requestCount)
	}

	return model.ServiceHealth{
		Name:        handlers.StringFromAny(row["service_name"]),
		Status:      s.getServiceStatus(errorRate),
		MetricCount: requestCount,
		LogCount:    0,
		TraceCount:  requestCount,
		ErrorCount:  errorCount,
		ErrorRate:   errorRate,
		LastSeen:    time.Now().UnixMilli(),
	}, nil
}

func (s *MetricService) GetServiceMetrics(ctx context.Context, teamUUID string, start, end time.Time) ([]model.ServiceMetric, error) {
	return s.repo.GetServiceMetrics(ctx, teamUUID, start, end)
}

func (s *MetricService) GetEndpointMetrics(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.EndpointMetric, error) {
	return s.repo.GetEndpointMetrics(ctx, teamUUID, start, end, serviceName)
}

func (s *MetricService) GetMetricsTimeSeries(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.TimeSeriesPoint, error) {
	return s.repo.GetMetricsTimeSeries(ctx, teamUUID, start, end, serviceName)
}

func (s *MetricService) GetMetricsSummary(ctx context.Context, teamUUID string, start, end time.Time) (model.MetricsSummary, error) {
	return s.repo.GetMetricsSummary(ctx, teamUUID, start, end)
}

func (s *MetricService) GetServiceTimeSeries(ctx context.Context, teamUUID string, start, end time.Time) ([]model.TimeSeriesPoint, error) {
	return s.repo.GetServiceTimeSeries(ctx, teamUUID, start, end)
}

func (s *MetricService) GetEndpointTimeSeries(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.TimeSeriesPoint, error) {
	return s.repo.GetEndpointTimeSeries(ctx, teamUUID, start, end, serviceName)
}

func (s *MetricService) GetServiceTopology(ctx context.Context, teamUUID string, start, end time.Time) (model.TopologyData, error) {
	nodes, err := s.repo.GetServiceTopologyNodes(ctx, teamUUID, start, end)
	if err != nil {
		return model.TopologyData{}, err
	}
	edges, err := s.repo.GetServiceTopologyEdges(ctx, teamUUID, start, end)
	if err != nil {
		return model.TopologyData{}, err
	}

	return model.TopologyData{
		Nodes: nodes,
		Edges: edges,
	}, nil
}

func (s *MetricService) GetSystemStatus(ctx context.Context) map[string]any {
	return map[string]any{
		"version": "v2-go",
		"tables":  []string{"spans", "logs", "incidents", "metrics", "deployments", "health_check_results"},
	}
}

func (s *MetricService) getServiceStatus(errorRate float64) string {
	if errorRate > 10 {
		return "CRITICAL"
	}
	if errorRate > 1 {
		return "WARNING"
	}
	return "HEALTHY"
}
