package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/infra/cursor"
	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/analytics"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/queryparser"
)

// teamIDString converts the int64 tenant id to the string form used by all
// sketch keys.
func teamIDString(teamID int64) string { return fmt.Sprintf("%d", teamID) }

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
	repo    Repository
	sketchQ *sketch.Querier
}

func NewService(repo Repository, sketchQ *sketch.Querier) Service {
	return &service{repo: repo, sketchQ: sketchQ}
}

func (s *service) GetOverview(ctx context.Context, teamID, startMs, endMs int64) (AIOverview, error) {
	summary, err := s.repo.GetOverview(ctx, teamID, startMs, endMs)
	if err != nil {
		return summary, err
	}
	s.attachOverviewP95(ctx, teamID, startMs, endMs, &summary)
	return summary, nil
}

func (s *service) GetOverviewTimeseries(ctx context.Context, teamID, startMs, endMs int64, step string) ([]AITrendPoint, error) {
	if step == "" {
		step = "5m"
	}
	rows, err := s.repo.GetOverviewTimeseries(ctx, teamID, startMs, endMs, step)
	if err != nil {
		return rows, err
	}
	s.attachTrendP95(ctx, teamID, startMs, endMs, step, rows)
	return rows, nil
}

// attachOverviewP95 merges SpanLatencyService sketches for the services
// observed in the AI overview window and writes p95 into the summary.
// Caveat: SpanLatencyService dim is not GenAI-filtered; it mirrors the
// ai/explorer trade-off (service names that mix AI and non-AI traffic will
// over-contribute). Documented in repository.go.
func (s *service) attachOverviewP95(ctx context.Context, teamID, startMs, endMs int64, summary *AIOverview) {
	if s.sketchQ == nil || summary == nil {
		return
	}
	prefixes := prefixesForServices(summary.Services)
	pcts, err := s.sketchQ.PercentilesByDimPrefix(ctx, sketch.SpanLatencyService, teamIDString(teamID), startMs, endMs, prefixes, 0.95)
	if err != nil {
		return
	}
	best := 0.0
	for _, v := range pcts {
		if len(v) == 1 && v[0] > best {
			best = v[0]
		}
	}
	summary.P95LatencyMs = best
}

// attachTrendP95 fills p95 per trend bucket using PercentilesTimeseries.
// Buckets without coverage keep their zero p95.
func (s *service) attachTrendP95(ctx context.Context, teamID, startMs, endMs int64, step string, rows []AITrendPoint) {
	if s.sketchQ == nil || len(rows) == 0 {
		return
	}
	series, err := s.sketchQ.PercentilesTimeseries(ctx, sketch.SpanLatencyService, teamIDString(teamID), startMs, endMs, stepSeconds(step), 0.95)
	if err != nil || len(series) == 0 {
		return
	}
	// Aggregate across all services per bucket — take the max as a
	// conservative representative (service-level sketches can't be merged
	// post-hoc without re-storing the sketch itself).
	perTs := make(map[int64]float64)
	for _, pts := range series {
		for _, p := range pts {
			if len(p.Values) == 0 {
				continue
			}
			if p.Values[0] > perTs[p.BucketTs] {
				perTs[p.BucketTs] = p.Values[0]
			}
		}
	}
	if len(perTs) == 0 {
		return
	}
	for i := range rows {
		ts := rows[i].TimeBucket.Unix()
		if v, ok := perTs[ts]; ok {
			rows[i].P95LatencyMs = v
		}
	}
}

func prefixesForServices(services []string) []string {
	if len(services) == 0 {
		return []string{""}
	}
	out := make([]string, 0, len(services))
	for _, svc := range services {
		if svc != "" {
			out = append(out, sketch.DimSpanService(svc))
		}
	}
	if len(out) == 0 {
		return []string{""}
	}
	return out
}

// stepSeconds maps the handler-provided step name to a bucket size in seconds.
// Unknown values fall through to 60s which matches SpanLatencyService's ingest
// bucket.
func stepSeconds(step string) int64 {
	switch strings.ToLower(strings.TrimSpace(step)) {
	case "1m", "minute":
		return 60
	case "5m":
		return 300
	case "15m":
		return 900
	case "1h", "hour":
		return 3600
	case "1d", "day":
		return 86400
	default:
		return 60
	}
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

	cur, _ := cursor.Decode[AICursor](req.Cursor)

	results, hasMore, err := s.repo.SearchRuns(ctx, q, limit, cur, req.OrderBy, req.OrderDir)
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
	s.attachOverviewP95(ctx, teamID, req.StartTime, req.EndTime, &summary)

	trend, err := s.repo.TrendRuns(ctx, q, req.Step)
	if err != nil {
		return AIExplorerResponse{}, err
	}
	s.attachTrendP95(ctx, teamID, req.StartTime, req.EndTime, req.Step, trend)

	facets, err := s.repo.FacetRuns(ctx, q)
	if err != nil {
		return AIExplorerResponse{}, err
	}

	var nextCursor string
	if hasMore && len(results) > 0 {
		last := results[len(results)-1]
		nextCursor = cursor.Encode(AICursor{StartTime: last.StartTime, SpanID: last.SpanID})
	}

	return AIExplorerResponse{
		Results:  results,
		Summary:  summary,
		Facets:   facets,
		Trend:    trend,
		PageInfo: AIPageInfo{Limit: limit, HasMore: hasMore, NextCursor: nextCursor},
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
