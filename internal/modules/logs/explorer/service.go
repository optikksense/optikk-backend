package explorer

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	queryparser "github.com/Optikk-Org/optikk-backend/internal/modules/explorer/queryparser"
	logshared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
	logsearch "github.com/Optikk-Org/optikk-backend/internal/modules/logs/search"
)

type Service struct {
	searchService *logsearch.Service
	logStats      *LogStatsService
}

func NewService(searchService *logsearch.Service, logStats *LogStatsService) *Service {
	return &Service{
		searchService: searchService,
		logStats:      logStats,
	}
}

func (s *Service) Query(ctx context.Context, req QueryRequest, teamID int64) (Response, error) {
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

	cursor, _ := logshared.ParseLogCursor(req.Cursor)

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
		PageInfo: PageInfo{HasMore: searchResult.HasMore, NextCursor: searchResult.NextCursor, Limit: limit},
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

var allowedFieldColumns = map[string]string{
	"severity_text": "severity_text",
	"service":       "service",
	"host":          "host",
	"pod":           "pod",
	"container":     "container",
	"scope_name":    "scope_name",
	"environment":   "environment",
}

// LogStatsService implements histogram, volume, stats, fields, and aggregate queries for logs.
type LogStatsService struct {
	repo *logStatsRepository
}

func newLogStatsService(db clickhouse.Conn) *LogStatsService {
	return &LogStatsService{repo: newLogStatsRepository(db)}
}

// GetLogVolume pivots the raw (bucket, severity, count) rows returned by the
// repository into per-bucket severity breakdowns. Keeps the SQL pure count()
// by grouping; the severity→column pivot happens here, in Go.
func (s *LogStatsService) GetLogVolume(ctx context.Context, f logshared.LogFilters, step string) (LogVolumeData, error) {
	raw, err := s.repo.GetLogVolume(ctx, f, step)
	if err != nil {
		return LogVolumeData{}, err
	}

	// Preserve bucket ordering as CH returned it.
	bucketIndex := make(map[string]int, len(raw))
	buckets := make([]LogVolumeBucket, 0)

	for _, row := range raw {
		idx, ok := bucketIndex[row.TimeBucket]
		if !ok {
			bucketIndex[row.TimeBucket] = len(buckets)
			buckets = append(buckets, LogVolumeBucket{TimeBucket: row.TimeBucket})
			idx = len(buckets) - 1
		}
		n := int64(row.Count) //nolint:gosec // count is domain-bounded
		buckets[idx].Total += n
		switch strings.ToUpper(row.Severity) {
		case "ERROR":
			buckets[idx].Errors += n
		case "WARN", "WARNING":
			buckets[idx].Warnings += n
		case "INFO":
			buckets[idx].Infos += n
		case "DEBUG":
			buckets[idx].Debugs += n
		case "FATAL":
			buckets[idx].Fatals += n
		}
	}

	return LogVolumeData{Buckets: buckets, Step: step}, nil
}

func (s *Service) GetLogStats(ctx context.Context, f logshared.LogFilters) (LogStats, error) {
	return s.logStats.GetLogStats(ctx, f)
}

func (s *LogStatsService) GetLogStats(ctx context.Context, f logshared.LogFilters) (LogStats, error) {
	rows, err := s.repo.GetLogStats(ctx, f)
	if err != nil {
		return LogStats{}, err
	}
	fields := map[string][]Facet{
		"level":        {},
		"service_name": {},
		"host":         {},
		"pod":          {},
		"scope_name":   {},
	}
	limits := map[string]int{
		"level":        100,
		"service_name": 50,
		"host":         50,
		"pod":          50,
		"scope_name":   50,
	}

	for _, row := range rows {
		facets := fields[row.Dim]
		limit := limits[row.Dim]
		if limit == 0 || len(facets) < limit {
			fields[row.Dim] = append(facets, Facet{Value: row.Value, Count: row.Count})
		}
	}

	var total int64
	for _, facet := range fields["level"] {
		total += facet.Count
	}

	return LogStats{Total: total, Fields: fields}, nil
}

func (s *LogStatsService) GetLogAggregate(ctx context.Context, f logshared.LogFilters, req LogAggregateRequest) (LogAggregateResponse, error) {
	query, err := buildLogAggregateQuery(req)
	if err != nil {
		return LogAggregateResponse{}, err
	}

	topRows, err := s.repo.GetTopGroups(ctx, f, query)
	if err != nil {
		return LogAggregateResponse{}, err
	}
	groups := make([]string, 0, len(topRows))
	for _, row := range topRows {
		if row.GroupValue != "" {
			groups = append(groups, row.GroupValue)
		}
	}

	// GetAggregateSeries fetches the "total count per (bucket, grp)" and —
	// when metric=error_rate — a second scan narrowed to severity IN
	// ('ERROR','FATAL'). Error rate is divided in Go. SQL never uses
	// sumIf / if / multiIf.
	totals, err := s.repo.GetAggregateSeries(ctx, f, query, groups, false)
	if err != nil {
		return LogAggregateResponse{}, err
	}
	var errorsByKey map[string]uint64
	if query.Metric == metricErrorRate {
		errorRows, eErr := s.repo.GetAggregateSeries(ctx, f, query, groups, true)
		if eErr != nil {
			return LogAggregateResponse{}, eErr
		}
		errorsByKey = make(map[string]uint64, len(errorRows))
		for _, er := range errorRows {
			errorsByKey[er.TimeBucket+"\x00"+er.GroupValue] = er.Count
		}
	}
	respRows := make([]LogAggregateRow, len(totals))
	for i, row := range totals {
		count := int64(row.Count) //nolint:gosec // domain-bounded
		var errRate float64
		if query.Metric == metricErrorRate && row.Count > 0 {
			errs := errorsByKey[row.TimeBucket+"\x00"+row.GroupValue]
			errRate = float64(errs) * 100.0 / float64(row.Count)
		}
		respRows[i] = LogAggregateRow{
			TimeBucket: utils.TimeFromAny(row.TimeBucket).UTC().Format(time.RFC3339),
			GroupValue: row.GroupValue,
			Count:      count,
			ErrorRate:  errRate,
		}
	}

	return LogAggregateResponse{
		GroupBy: query.GroupBy,
		Step:    query.Step,
		Metric:  query.Metric,
		Rows:    respRows,
	}, nil
}

func buildLogAggregateQuery(req LogAggregateRequest) (logAggregateQuery, error) {
	groupBy := req.GroupBy
	if groupBy == "" {
		groupBy = "service"
	}
	groupCol, ok := allowedFieldColumns[groupBy]
	if !ok {
		return logAggregateQuery{}, fmt.Errorf("invalid groupBy field: %s", req.GroupBy)
	}
	step := req.Step
	if step == "" {
		step = "5m"
	}
	topN := req.TopN
	if topN <= 0 || topN > 100 {
		topN = 20
	}
	metric := req.Metric
	if metric == "" {
		metric = "count"
	}
	if metric != "count" && metric != metricErrorRate {
		return logAggregateQuery{}, fmt.Errorf("invalid metric: %s", req.Metric)
	}

	return logAggregateQuery{
		GroupBy:  groupBy,
		GroupCol: groupCol,
		Step:     step,
		TopN:     topN,
		Metric:   metric,
	}, nil
}
