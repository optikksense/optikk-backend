package explorer

import (
	"context"
	"fmt"
	"strings"

	logshared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
	logsearch "github.com/Optikk-Org/optikk-backend/internal/modules/logs/search"

	exploreranalytics "github.com/Optikk-Org/optikk-backend/internal/modules/explorer/analytics"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/queryparser"
)

type Service struct {
	searchService     *logsearch.Service
	logStats          *LogStatsService
	explorerAnalytics *exploreranalytics.Service
}

func NewService(searchService *logsearch.Service, logStats *LogStatsService, explorerAnalytics *exploreranalytics.Service) *Service {
	return &Service{
		searchService:     searchService,
		logStats:          logStats,
		explorerAnalytics: explorerAnalytics,
	}
}

// Query handles the explorer request. If groupBy/aggregations are set, delegates to
// the unified analytics engine. Otherwise, returns the standard list view.
func (s *Service) Query(ctx context.Context, req QueryRequest, teamID int64) (any, error) {
	if len(req.GroupBy) > 0 && len(req.Aggregations) > 0 {
		return s.queryAnalytics(ctx, req, teamID)
	}
	return s.queryList(ctx, req, teamID)
}

func (s *Service) queryAnalytics(ctx context.Context, req QueryRequest, teamID int64) (*exploreranalytics.AnalyticsResult, error) {
	analyticsReq := exploreranalytics.AnalyticsRequest{
		Query:        req.Query,
		StartTime:    req.StartTime,
		EndTime:      req.EndTime,
		GroupBy:      req.GroupBy,
		Aggregations: req.Aggregations,
		OrderBy:      req.OrderBy,
		OrderDir:     req.OrderDir,
		Limit:        req.Limit,
		Step:         req.Step,
		VizMode:      req.VizMode,
	}
	return s.explorerAnalytics.RunQuery(ctx, teamID, analyticsReq, "logs")
}

func (s *Service) queryList(ctx context.Context, req QueryRequest, teamID int64) (Response, error) {
	filters, err := buildFiltersFromQuery(req, teamID)
	if err != nil {
		return Response{}, fmt.Errorf("logExplorer.Query.parseQuery: %w", err)
	}

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
		return Response{}, fmt.Errorf("logExplorer.Query.GetLogs: %w", err)
	}

	stats, err := s.logStats.GetLogStats(ctx, filters)
	if err != nil {
		return Response{}, fmt.Errorf("logExplorer.Query.GetStats: %w", err)
	}

	volume, err := s.logStats.GetLogVolume(ctx, filters, req.Step)
	if err != nil {
		return Response{}, fmt.Errorf("logExplorer.Query.GetVolume: %w", err)
	}

	aggregate, err := s.logStats.GetLogAggregate(ctx, filters, LogAggregateRequest{
		GroupBy: "service",
		Step:    req.Step,
		TopN:    8,
		Metric:  "error_rate",
	})
	if err != nil {
		aggregate = LogAggregateResponse{}
	}

	summary := Summary{
		TotalLogs:  stats.Total,
		ErrorLogs:  countFacetValues(stats.Fields["level"], "ERROR", "FATAL"),
		WarnLogs:   countFacetValues(stats.Fields["level"], "WARN", "WARNING"),
		ServiceCnt: len(stats.Fields["service_name"]),
	}

	return Response{
		Results: searchResult.Logs,
		Summary: summary,
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

// buildFiltersFromQuery parses the query string into LogFilters.
func buildFiltersFromQuery(req QueryRequest, teamID int64) (logshared.LogFilters, error) {
	filters := logshared.LogFilters{
		TeamID:  teamID,
		StartMs: req.StartTime,
		EndMs:   req.EndTime,
	}

	if req.Query == "" {
		return filters, nil
	}

	node, err := queryparser.Parse(req.Query)
	if err != nil {
		return filters, fmt.Errorf("invalid query: %w", err)
	}
	if node == nil {
		return filters, nil
	}

	// Walk the AST to extract filters that map directly to LogFilters fields.
	// For complex boolean expressions, fall back to building a raw WHERE clause.
	extractSimpleFilters(node, &filters)

	return filters, nil
}

// extractSimpleFilters walks simple field:value nodes and populates LogFilters.
// For AND nodes, it recurses into children. For other node types (OR, NOT, etc.),
// it converts to raw attribute filters.
func extractSimpleFilters(node queryparser.Node, f *logshared.LogFilters) {
	switch n := node.(type) {
	case *queryparser.AndNode:
		for _, child := range n.Children {
			extractSimpleFilters(child, f)
		}
	case *queryparser.FieldMatch:
		mapFieldToFilter(n.Field, n.Value, f)
	case *queryparser.FreeText:
		if f.Search == "" {
			f.Search = n.Text
		} else {
			f.Search += " " + n.Text
		}
	case *queryparser.NotNode:
		if fm, ok := n.Child.(*queryparser.FieldMatch); ok {
			mapFieldToExcludeFilter(fm.Field, fm.Value, f)
		}
	}
}

func mapFieldToFilter(field, value string, f *logshared.LogFilters) {
	lower := strings.ToLower(field)
	switch lower {
	case "service":
		f.Services = append(f.Services, value)
	case "status", "level", "severity":
		f.Severities = append(f.Severities, value)
	case "host":
		f.Hosts = append(f.Hosts, value)
	case "pod":
		f.Pods = append(f.Pods, value)
	case "container":
		f.Containers = append(f.Containers, value)
	case "environment":
		f.Environments = append(f.Environments, value)
	case "trace_id":
		f.TraceID = value
	case "span_id":
		f.SpanID = value
	default:
		if strings.HasPrefix(field, "@") {
			f.AttributeFilters = append(f.AttributeFilters, logshared.LogAttributeFilter{
				Key: field[1:], Value: value, Op: "eq",
			})
		}
	}
}

func mapFieldToExcludeFilter(field, value string, f *logshared.LogFilters) {
	lower := strings.ToLower(field)
	switch lower {
	case "service":
		f.ExcludeServices = append(f.ExcludeServices, value)
	case "status", "level", "severity":
		f.ExcludeSeverities = append(f.ExcludeSeverities, value)
	case "host":
		f.ExcludeHosts = append(f.ExcludeHosts, value)
	default:
		if strings.HasPrefix(field, "@") {
			f.AttributeFilters = append(f.AttributeFilters, logshared.LogAttributeFilter{
				Key: field[1:], Value: value, Op: "neq",
			})
		}
	}
}

func countFacetValues(facets []Facet, values ...string) int64 {
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
