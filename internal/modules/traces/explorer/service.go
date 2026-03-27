package explorer

import (
	"context"
	"fmt"

	spanlivetail "github.com/Optikk-Org/optikk-backend/internal/modules/traces/livetail"
	spantraces "github.com/Optikk-Org/optikk-backend/internal/modules/traces/query"
)

type tracesQueryService interface {
	SearchTraces(ctx context.Context, filters spantraces.TraceFilters, limit int, cursorRaw string, offset int) (spantraces.TraceSearchResult, error)
	GetExplorerFacets(ctx context.Context, filters spantraces.TraceFilters) ([]spantraces.TraceFacet, error)
	GetExplorerTrend(ctx context.Context, filters spantraces.TraceFilters, step string) ([]spantraces.TraceTrendBucket, error)
}

type Service struct {
	tracesService   tracesQueryService
	liveTailService *spanlivetail.Service
}

func NewService(tracesService tracesQueryService, liveTailService *spanlivetail.Service) *Service {
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
		bucket := FacetBucket{
			Value: facet.Value,
			Count: facet.Count,
		}
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

func (s *Service) Poll(ctx context.Context, teamID int64, filters spanlivetail.LiveTailFilters, sinceTime any) ([]spanlivetail.LiveSpan, error) {
	return nil, nil
}
