package explorer

import (
	"context"
	"fmt"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/queryparser"
	spantraces "github.com/Optikk-Org/optikk-backend/internal/modules/traces/query"
)

type tracesQueryService interface {
	SearchTraces(ctx context.Context, filters spantraces.TraceFilters, limit int, cursorRaw string, offset int) (spantraces.TraceSearchResult, error)
	GetExplorerFacets(ctx context.Context, filters spantraces.TraceFilters) ([]spantraces.TraceFacet, error)
	GetExplorerTrend(ctx context.Context, filters spantraces.TraceFilters, step string) ([]spantraces.TraceTrendBucket, error)
}

type Service struct {
	tracesService     tracesQueryService
}

func NewService(tracesService tracesQueryService) *Service {
	return &Service{
		tracesService:     tracesService,
	}
}

func (s *Service) Query(ctx context.Context, req QueryRequest, teamID int64) (Response, error) {
	filters, err := buildFiltersFromQuery(req, teamID)
	if err != nil {
		return Response{}, fmt.Errorf("explorer.Query.parseQuery: %w", err)
	}

	limit := req.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	result, err := s.tracesService.SearchTraces(ctx, filters, limit, req.Cursor, req.Offset)
	if err != nil {
		return Response{}, fmt.Errorf("explorer.Query.SearchTraces: %w", err)
	}

	facets, err := s.tracesService.GetExplorerFacets(ctx, filters)
	if err != nil {
		return Response{}, fmt.Errorf("explorer.Query.GetFacets: %w", err)
	}
	groupedFacets := ExplorerFacets{
		ServiceName:   []FacetBucket{},
		Status:        []FacetBucket{},
		OperationName: []FacetBucket{},
	}
	for _, facet := range facets {
		bucket := FacetBucket{Value: facet.Value, Count: facet.Count}
		switch facet.Key {
		case "service_name":
			groupedFacets.ServiceName = append(groupedFacets.ServiceName, bucket)
		case "status":
			groupedFacets.Status = append(groupedFacets.Status, bucket)
		case "operation_name":
			groupedFacets.OperationName = append(groupedFacets.OperationName, bucket)
		}
	}

	trend, err := s.tracesService.GetExplorerTrend(ctx, filters, req.Step)
	if err != nil {
		return Response{}, fmt.Errorf("explorer.Query.GetTrend: %w", err)
	}

	return Response{
		Results:  result.Traces,
		Summary:  result.Summary,
		Facets:   groupedFacets,
		Trend:    trend,
		PageInfo: PageInfo{Total: result.Total, HasMore: result.HasMore, NextCursor: result.NextCursor, Offset: result.Offset, Limit: limit},
		Correlations: Correlations{
			TopServices:   groupedFacets.ServiceName,
			TopOperations: groupedFacets.OperationName,
		},
	}, nil
}

// buildFiltersFromQuery parses the query string into TraceFilters.
func buildFiltersFromQuery(req QueryRequest, teamID int64) (spantraces.TraceFilters, error) {
	filters := spantraces.TraceFilters{
		TeamID:     teamID,
		StartMs:    req.StartTime,
		EndMs:      req.EndTime,
		SearchMode: "all",
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

	extractTraceFilters(node, &filters)
	return filters, nil
}

// extractTraceFilters walks simple field:value nodes and populates TraceFilters.
func extractTraceFilters(node queryparser.Node, f *spantraces.TraceFilters) {
	switch n := node.(type) {
	case *queryparser.AndNode:
		for _, child := range n.Children {
			extractTraceFilters(child, f)
		}
	case *queryparser.FieldMatch:
		mapTraceField(n.Field, n.Value, f)
	case *queryparser.FreeText:
		if f.SearchText == "" {
			f.SearchText = n.Text
		} else {
			f.SearchText += " " + n.Text
		}
	case *queryparser.ComparisonMatch:
		mapTraceComparison(n.Field, n.Op, n.Value, f)
	}
}

func mapTraceField(field, value string, f *spantraces.TraceFilters) {
	lower := strings.ToLower(field)
	switch lower {
	case "service", "service_name":
		f.Services = append(f.Services, value)
	case "status":
		f.Status = value
	case "operation", "operation_name":
		f.Operation = value
	case "http.method":
		f.HTTPMethod = value
	case "http.status_code":
		f.HTTPStatus = value
	case "span.kind", "kind":
		f.SpanKind = value
	case "name":
		f.SpanName = value
	case "trace_id":
		f.TraceID = value
	default:
		if strings.HasPrefix(field, "@") {
			f.AttributeFilters = append(f.AttributeFilters, spantraces.SpanAttributeFilter{
				Key: field[1:], Value: value, Op: "eq",
			})
		}
	}
}

func mapTraceComparison(field string, op queryparser.ComparisonOp, value string, f *spantraces.TraceFilters) {
	lower := strings.ToLower(field)
	switch lower {
	case "duration":
		switch op {
		case queryparser.OpGT, queryparser.OpGTE:
			f.MinDuration = value
		case queryparser.OpLT, queryparser.OpLTE:
			f.MaxDuration = value
		}
	}
}
