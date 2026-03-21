package explorer

import (
	"context"

	spanlivetail "github.com/observability/observability-backend-go/internal/modules/spans/livetail"
	spantraces "github.com/observability/observability-backend-go/internal/modules/spans/traces"
)

type Service struct {
	tracesService   *spantraces.Service
	liveTailService *spanlivetail.Service
}

func NewService(tracesService *spantraces.Service, liveTailService *spanlivetail.Service) *Service {
	return &Service{
		tracesService:   tracesService,
		liveTailService: liveTailService,
	}
}

func (s *Service) Query(ctx context.Context, req QueryRequest, teamID int64) (Response, error) {
	filters := mapToTraceFilters(req, teamID)
	limit := req.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	result, err := s.tracesService.SearchTraces(ctx, filters, limit, req.Cursor, req.Offset)
	if err != nil {
		return Response{}, err
	}

	facets, err := s.tracesService.GetExplorerFacets(ctx, filters)
	if err != nil {
		return Response{}, err
	}
	groupedFacets := make(map[string][]FacetBucket)
	for _, facet := range facets {
		groupedFacets[facet.Key] = append(groupedFacets[facet.Key], FacetBucket{
			Value: facet.Value,
			Count: facet.Count,
		})
	}

	trend, err := s.tracesService.GetExplorerTrend(ctx, filters, req.Step)
	if err != nil {
		return Response{}, err
	}

	return Response{
		Results:  result.Traces,
		Summary:  result.Summary,
		Facets:   groupedFacets,
		Trend:    trend,
		PageInfo: PageInfo{Total: result.Total, HasMore: result.HasMore, NextCursor: result.NextCursor, Offset: result.Offset, Limit: limit},
		Correlations: map[string]any{
			"topServices":   groupedFacets["service_name"],
			"topOperations": groupedFacets["operation_name"],
		},
	}, nil
}

func (s *Service) Poll(ctx context.Context, teamID int64, filters spanlivetail.LiveTailFilters, sinceTime any) ([]spanlivetail.LiveSpan, error) {
	return nil, nil
}
