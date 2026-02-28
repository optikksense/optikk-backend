package impl

import (
	"fmt"

	"github.com/observability/observability-backend-go/internal/modules/insights/model"
	"github.com/observability/observability-backend-go/internal/modules/insights/store"
)

// InsightService provides business logic orchestration for insights.
type InsightService struct {
	repo store.Repository
}

// NewService creates a new InsightService.
func NewService(repo store.Repository) *InsightService {
	return &InsightService{repo: repo}
}

func (s *InsightService) GetInsightSloSli(teamUUID string, startMs, endMs int64, serviceName string) (*model.SloSliResponse, error) {
	summary, timeseries, err := s.repo.GetInsightSloSli(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		fmt.Println("[SLO ERROR]", err)
		return nil, err
	}

	errorBudgetRemaining := 100.0
	if summary.AvailabilityPercent < 99.9 {
		errorBudgetRemaining = summary.AvailabilityPercent
	}

	return &model.SloSliResponse{
		Objectives: model.Objectives{
			AvailabilityTarget: 99.9,
			P95LatencyTargetMs: 300.0,
		},
		Status: model.SloStatus{
			AvailabilityPercent:         summary.AvailabilityPercent,
			P95LatencyMs:                summary.P95LatencyMs,
			ErrorBudgetRemainingPercent: errorBudgetRemaining,
			Compliant:                   summary.AvailabilityPercent >= 99.9 && summary.P95LatencyMs <= 300.0,
		},
		Summary:    summary,
		Timeseries: timeseries,
	}, nil
}

func (s *InsightService) GetInsightLogsStream(teamUUID string, startMs, endMs int64, limit int) (*model.LogsStreamResponse, error) {
	stream, total, volume, levelFacets, serviceFacets, err := s.repo.GetInsightLogsStream(teamUUID, startMs, endMs, limit)
	if err != nil {
		return nil, err
	}

	correlated := int64(0)
	for _, row := range stream {
		if row.TraceID != "" {
			correlated++
		}
	}
	uncorrelated := int64(len(stream)) - correlated
	if uncorrelated < 0 {
		uncorrelated = 0
	}
	ratio := 0.0
	if len(stream) > 0 {
		ratio = float64(correlated) * 100.0 / float64(len(stream))
	}

	return &model.LogsStreamResponse{
		Stream:       stream,
		Total:        total,
		VolumeTrends: volume,
		TraceCorrelation: model.TraceCorrelation{
			TraceCorrelatedLogs: correlated,
			UncorrelatedLogs:    uncorrelated,
			CorrelationRatio:    ratio,
		},
		Facets: model.LogFacets{
			Levels:   levelFacets,
			Services: serviceFacets,
		},
	}, nil
}
