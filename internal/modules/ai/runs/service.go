package runs

import (
	"context"
	"fmt"

	aishared "github.com/Optikk-Org/optikk-backend/internal/modules/ai/shared"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/analytics"
)

// Service defines the business logic for the AI runs module.
type Service interface {
	QueryRuns(ctx context.Context, teamID int64, req QueryRequest) (AIExplorerResponse, error)
	RunAnalytics(ctx context.Context, teamID int64, req analytics.AnalyticsRequest) (*analytics.AnalyticsResult, error)
	GetRunDetail(ctx context.Context, teamID int64, runID string) (*aishared.AIRunDetail, error)
	GetRunRelated(ctx context.Context, teamID int64, runID string) (*AIRunRelated, error)
}

type service struct {
	repo Repository
}

// NewService creates a new runs service.
func NewService(repo Repository) Service {
	return &service{repo: repo}
}

func (s *service) QueryRuns(ctx context.Context, teamID int64, req QueryRequest) (AIExplorerResponse, error) {
	queryWhere, queryArgs, err := aishared.CompileAIQuery(req.Query)
	if err != nil {
		return AIExplorerResponse{}, err
	}

	limit := req.Limit
	if limit <= 0 || limit > aishared.MaxPageSize {
		limit = aishared.DefaultPageSize
	}

	q := aishared.QueryContext{
		TeamID: teamID,
		Start:  req.StartTime,
		End:    req.EndTime,
		Where:  queryWhere,
		Args:   queryArgs,
	}

	results, total, err := s.repo.SearchRuns(ctx, q, limit, req.Offset, req.OrderBy, req.OrderDir)
	if err != nil {
		return AIExplorerResponse{}, err
	}
	for idx := range results {
		sanitizeRunRow(&results[idx])
	}

	summary, err := s.repo.SummarizeRuns(ctx, q)
	if err != nil {
		return AIExplorerResponse{}, err
	}

	trend, err := s.repo.TrendRuns(ctx, q, req.Step)
	if err != nil {
		return AIExplorerResponse{}, err
	}

	facets, err := s.repo.FacetRuns(ctx, q)
	if err != nil {
		return AIExplorerResponse{}, err
	}

	return AIExplorerResponse{
		Results:  results,
		Summary:  summary,
		Facets:   facets,
		Trend:    trend,
		PageInfo: AIPageInfo{Total: total, Offset: req.Offset, Limit: limit, HasMore: uint64(req.Offset+limit) < total},
		Correlations: AICorrelations{
			TopModels:  facets.Provider,
			TopPrompts: facets.PromptTemplate,
		},
	}, nil
}

func (s *service) RunAnalytics(ctx context.Context, teamID int64, req analytics.AnalyticsRequest) (*analytics.AnalyticsResult, error) {
	queryWhere, queryArgs, err := aishared.CompileAIQuery(req.Query)
	if err != nil {
		return nil, err
	}
	return s.repo.RunAnalytics(ctx, teamID, req, queryWhere, queryArgs)
}

func (s *service) GetRunDetail(ctx context.Context, teamID int64, runID string) (*aishared.AIRunDetail, error) {
	traceID, spanID, err := aishared.ParseRunID(runID)
	if err != nil {
		return nil, fmt.Errorf("invalid run id: %w", err)
	}
	detail, err := s.repo.GetRunDetail(ctx, teamID, traceID, spanID)
	if err != nil || detail == nil {
		return detail, err
	}
	sanitizeRunDetail(detail)
	return detail, nil
}

func (s *service) GetRunRelated(ctx context.Context, teamID int64, runID string) (*AIRunRelated, error) {
	traceID, spanID, err := aishared.ParseRunID(runID)
	if err != nil {
		return nil, fmt.Errorf("invalid run id: %w", err)
	}
	detail, err := s.repo.GetRunDetail(ctx, teamID, traceID, spanID)
	if err != nil {
		return nil, err
	}
	if detail == nil {
		return nil, nil
	}
	sanitizeRunDetail(detail)

	logRows, err := s.repo.GetRunLogs(ctx, teamID, traceID, spanID, aishared.DefaultLogLimit)
	if err != nil {
		return nil, err
	}
	logs := make([]AIRelatedLog, 0, len(logRows))
	for _, row := range logRows {
		logs = append(logs, AIRelatedLog{
			Timestamp:    row.Timestamp.Format("2006-01-02T15:04:05.000Z"),
			SeverityText: row.SeverityText,
			Body:         aishared.SanitizeSnippet(row.Body),
			ServiceName:  row.ServiceName,
			TraceID:      row.TraceID,
			SpanID:       row.SpanID,
		})
	}

	alerts, err := s.repo.GetMatchingAlerts(ctx, teamID, detail)
	if err != nil {
		return nil, err
	}

	return &AIRunRelated{
		RunID:          detail.RunID,
		TraceID:        detail.TraceID,
		SpanID:         detail.SpanID,
		ServiceName:    detail.ServiceName,
		ServiceVersion: detail.ServiceVersion,
		Environment:    detail.Environment,
		Logs:           logs,
		Alerts:         alerts,
	}, nil
}

func sanitizeRunRow(row *aishared.AIRunRow) {
	row.Provider = aishared.FallbackString(row.Provider, "Unknown")
	row.RequestModel = aishared.FallbackString(row.RequestModel, "Unknown")
	row.Operation = aishared.FallbackString(row.Operation, row.ServiceName)
	row.QualityBucket = aishared.FallbackString(row.QualityBucket, "unscored")
}

func sanitizeRunDetail(detail *aishared.AIRunDetail) {
	sanitizeRunRow(&detail.AIRunRow)
	detail.PromptSnippet = aishared.SanitizeSnippet(detail.PromptSnippet)
	detail.ResponseSnippet = aishared.SanitizeSnippet(detail.ResponseSnippet)
}
