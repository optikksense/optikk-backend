package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/analytics"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/queryparser"
)

type Service interface {
	GetOverview(ctx context.Context, teamID, startMs, endMs int64) (AIOverview, error)
	GetOverviewTimeseries(ctx context.Context, teamID, startMs, endMs int64, step string) ([]AITrendPoint, error)
	GetTopModels(ctx context.Context, teamID, startMs, endMs int64) ([]AIModelBreakdown, error)
	GetTopPrompts(ctx context.Context, teamID, startMs, endMs int64) ([]AIPromptBreakdown, error)
	GetQualitySummary(ctx context.Context, teamID, startMs, endMs int64) (AIQualitySummary, error)
	QueryRuns(ctx context.Context, teamID int64, req QueryRequest) (AIExplorerResponse, error)
	RunAnalytics(ctx context.Context, teamID int64, req analytics.AnalyticsRequest) (*analytics.AnalyticsResult, error)
	GetRunDetail(ctx context.Context, teamID int64, runID string) (*AIRunDetail, error)
	GetRunRelated(ctx context.Context, teamID int64, runID string) (*AIRunRelated, error)
}

type service struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &service{repo: repo}
}

func (s *service) GetOverview(ctx context.Context, teamID, startMs, endMs int64) (AIOverview, error) {
	return s.repo.GetOverview(ctx, teamID, startMs, endMs)
}

func (s *service) GetOverviewTimeseries(ctx context.Context, teamID, startMs, endMs int64, step string) ([]AITrendPoint, error) {
	if step == "" {
		step = "5m"
	}
	return s.repo.GetOverviewTimeseries(ctx, teamID, startMs, endMs, step)
}

func (s *service) GetTopModels(ctx context.Context, teamID, startMs, endMs int64) ([]AIModelBreakdown, error) {
	return s.repo.GetTopModels(ctx, teamID, startMs, endMs, defaultBreakdownLimit)
}

func (s *service) GetTopPrompts(ctx context.Context, teamID, startMs, endMs int64) ([]AIPromptBreakdown, error) {
	return s.repo.GetTopPrompts(ctx, teamID, startMs, endMs, defaultBreakdownLimit)
}

func (s *service) GetQualitySummary(ctx context.Context, teamID, startMs, endMs int64) (AIQualitySummary, error) {
	return s.repo.GetQualitySummary(ctx, teamID, startMs, endMs)
}

func (s *service) QueryRuns(ctx context.Context, teamID int64, req QueryRequest) (AIExplorerResponse, error) {
	queryWhere, queryArgs, err := compileAIQuery(req.Query)
	if err != nil {
		return AIExplorerResponse{}, err
	}

	limit := req.Limit
	if limit <= 0 || limit > maxPageSize {
		limit = defaultPageSize
	}

	q := queryContext{
		teamID: teamID,
		start:  req.StartTime,
		end:    req.EndTime,
		where:  queryWhere,
		args:   queryArgs,
	}

	results, total, err := s.repo.SearchRuns(ctx, q, limit, req.Offset, req.OrderBy, req.OrderDir)
	if err != nil {
		return AIExplorerResponse{}, err
	}
	for idx := range results {
		s.sanitizeRunRow(&results[idx])
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
	queryWhere, queryArgs, err := compileAIQuery(req.Query)
	if err != nil {
		return nil, err
	}
	return s.repo.RunAnalytics(ctx, teamID, req, queryWhere, queryArgs)
}

func (s *service) GetRunDetail(ctx context.Context, teamID int64, runID string) (*AIRunDetail, error) {
	traceID, spanID, err := parseRunID(runID)
	if err != nil {
		return nil, err
	}
	detail, err := s.repo.GetRunDetail(ctx, teamID, traceID, spanID)
	if err != nil || detail == nil {
		return detail, err
	}
	s.sanitizeRunDetail(detail)
	return detail, nil
}

func (s *service) GetRunRelated(ctx context.Context, teamID int64, runID string) (*AIRunRelated, error) {
	traceID, spanID, err := parseRunID(runID)
	if err != nil {
		return nil, err
	}
	detail, err := s.repo.GetRunDetail(ctx, teamID, traceID, spanID)
	if err != nil {
		return nil, err
	}
	if detail == nil {
		return nil, nil
	}
	s.sanitizeRunDetail(detail)

	logs, err := s.repo.GetRunLogs(ctx, teamID, traceID, spanID, defaultLogLimit)
	if err != nil {
		return nil, err
	}
	for idx := range logs {
		logs[idx].Body = sanitizeSnippet(logs[idx].Body)
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

func compileAIQuery(raw string) (string, []any, error) {
	if strings.TrimSpace(raw) == "" {
		return "", nil, nil
	}

	node, err := queryparser.Parse(raw)
	if err != nil {
		return "", nil, fmt.Errorf("invalid query: %w", err)
	}
	if node == nil {
		return "", nil, nil
	}

	compiled, err := queryparser.Compile(node, queryparser.AISchema{})
	if err != nil {
		return "", nil, fmt.Errorf("query compilation error: %w", err)
	}
	return compiled.Where, compiled.Args, nil
}

func parseRunID(runID string) (string, string, error) {
	parts := strings.SplitN(runID, ":", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("invalid run id")
	}
	return parts[0], parts[1], nil
}

func (s *service) sanitizeRunRow(row *AIRunRow) {
	row.Provider = fallbackString(row.Provider, "Unknown")
	row.RequestModel = fallbackString(row.RequestModel, "Unknown")
	row.Operation = fallbackString(row.Operation, row.ServiceName)
	row.QualityBucket = fallbackString(row.QualityBucket, "unscored")
}

func (s *service) sanitizeRunDetail(detail *AIRunDetail) {
	s.sanitizeRunRow(&detail.AIRunRow)
	detail.PromptSnippet = sanitizeSnippet(detail.PromptSnippet)
	detail.ResponseSnippet = sanitizeSnippet(detail.ResponseSnippet)
}

func fallbackString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

var emailPattern = regexp.MustCompile(`[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}`)
var secretPattern = regexp.MustCompile(`(?i)(bearer\s+[a-z0-9._\-]+|sk-[a-z0-9]+|api[_-]?key["'=:\s]+[a-z0-9_\-]+)`)

func sanitizeSnippet(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	clean := strings.TrimSpace(value)
	clean = emailPattern.ReplaceAllString(clean, "[redacted-email]")
	clean = secretPattern.ReplaceAllString(clean, "[redacted-secret]")
	clean = strings.Join(strings.Fields(clean), " ")
	if len(clean) > maxSnippetLength {
		return clean[:maxSnippetLength] + "..."
	}
	return clean
}

func prettyTargetRef(raw map[string]any) string {
	b, err := json.Marshal(raw)
	if err != nil {
		return ""
	}
	return string(b)
}
