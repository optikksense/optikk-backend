package explorer

import (
	"context"
	"strings"

	loganalytics "github.com/observability/observability-backend-go/internal/modules/log/analytics"
	logsearch "github.com/observability/observability-backend-go/internal/modules/log/search"
	logshared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"
)

type Service struct {
	searchService    *logsearch.Service
	analyticsService *loganalytics.Service
}

func NewService(searchService *logsearch.Service, analyticsService *loganalytics.Service) *Service {
	return &Service{
		searchService:    searchService,
		analyticsService: analyticsService,
	}
}

func (s *Service) Query(ctx context.Context, req QueryRequest, teamID int64) (Response, error) {
	filters := mapToLogFilters(req, teamID)
	limit := req.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	direction := strings.ToLower(strings.TrimSpace(req.Direction))
	if direction != "asc" {
		direction = "desc"
	}

	cursor := logshared.LogCursor{Offset: req.Offset}
	if parsedCursor, ok := logshared.ParseLogCursor(req.Cursor); ok {
		cursor = parsedCursor
	}

	searchResult, err := s.searchService.GetLogs(ctx, filters, limit, direction, cursor)
	if err != nil {
		return Response{}, err
	}

	stats, err := s.analyticsService.GetLogStats(ctx, filters)
	if err != nil {
		return Response{}, err
	}

	volume, err := s.analyticsService.GetLogVolume(ctx, filters, req.Step)
	if err != nil {
		return Response{}, err
	}

	aggregate, err := s.analyticsService.GetLogAggregate(ctx, filters, loganalytics.LogAggregateRequest{
		GroupBy: "service",
		Step:    req.Step,
		TopN:    8,
		Metric:  "error_rate",
	})
	if err != nil {
		aggregate = loganalytics.LogAggregateResponse{}
	}

	summary := Summary{
		TotalLogs:  stats.Total,
		ErrorLogs:  countFacetValues(stats.Fields["level"], "ERROR", "FATAL"),
		WarnLogs:   countFacetValues(stats.Fields["level"], "WARN", "WARNING"),
		ServiceCnt: len(stats.Fields["service_name"]),
	}

	return Response{
		Results:  searchResult.Logs,
		Summary:  summary,
		Facets: ExplorerFacets{
			Level:       stats.Fields["level"],
			ServiceName: stats.Fields["service_name"],
			Host:        stats.Fields["host"],
			Pod:         stats.Fields["pod"],
			ScopeName:   stats.Fields["scope_name"],
		},
		Trend:    volume,
		PageInfo: PageInfo{Total: searchResult.Total, HasMore: searchResult.HasMore, NextCursor: searchResult.NextCursor, Offset: cursor.Offset, Limit: limit},
		Correlations: ExplorerCorrelations{
			ServiceErrorRate: aggregate,
		},
	}, nil
}

func countFacetValues(facets []loganalytics.Facet, values ...string) int64 {
	if len(facets) == 0 {
		return 0
	}

	lookup := make(map[string]struct{}, len(values))
	for _, value := range values {
		lookup[strings.ToUpper(strings.TrimSpace(value))] = struct{}{}
	}

	var total int64
	for _, facet := range facets {
		if _, ok := lookup[strings.ToUpper(strings.TrimSpace(facet.Value))]; ok {
			total += facet.Count
		}
	}
	return total
}
