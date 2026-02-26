package impl

import (
	"context"

	"github.com/observability/observability-backend-go/modules/spans/model"
	"github.com/observability/observability-backend-go/modules/spans/store"
)

type TraceService struct {
	repo store.Repository
}

func NewService(repo store.Repository) *TraceService {
	return &TraceService{repo: repo}
}

func (s *TraceService) GetTraces(ctx context.Context, teamUUID string, startMs, endMs int64, limit, offset int, filters model.TraceFilters) (model.TraceSearchResponse, error) {
	filters.TeamUUID = teamUUID
	filters.StartMs = startMs
	filters.EndMs = endMs

	traces, total, summary, err := s.repo.GetTraces(ctx, filters, limit, offset)
	if err != nil {
		return model.TraceSearchResponse{}, err
	}

	return model.TraceSearchResponse{
		Traces:  traces,
		HasMore: len(traces) >= limit,
		Offset:  offset,
		Limit:   limit,
		Total:   total,
		Summary: summary,
	}, nil
}

func (s *TraceService) GetTraceSpans(ctx context.Context, teamUUID, traceID string) ([]model.Span, error) {
	return s.repo.GetTraceSpans(ctx, teamUUID, traceID)
}

func (s *TraceService) GetServiceDependencies(ctx context.Context, teamUUID string, startMs, endMs int64) ([]model.ServiceDependency, error) {
	return s.repo.GetServiceDependencies(ctx, teamUUID, startMs, endMs)
}

func (s *TraceService) GetErrorGroups(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]model.ErrorGroup, error) {
	return s.repo.GetErrorGroups(ctx, teamUUID, startMs, endMs, serviceName, limit)
}

func (s *TraceService) GetErrorTimeSeries(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string) ([]model.ErrorTimeSeries, error) {
	return s.repo.GetErrorTimeSeries(ctx, teamUUID, startMs, endMs, serviceName)
}

func (s *TraceService) GetLatencyHistogram(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName, operationName string) ([]model.LatencyHistogramBucket, error) {
	return s.repo.GetLatencyHistogram(ctx, teamUUID, startMs, endMs, serviceName, operationName)
}

func (s *TraceService) GetLatencyHeatmap(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string) ([]model.LatencyHeatmapPoint, error) {
	return s.repo.GetLatencyHeatmap(ctx, teamUUID, startMs, endMs, serviceName)
}
