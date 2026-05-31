package traces

import "context"

type Service interface {
	// explorer
	Query(ctx context.Context, req QueryRequest) (QueryResponse, error)

	// facets
	QueryFacets(ctx context.Context, req FacetsRequest) (Facets, error)

	// trend
	QueryTrend(ctx context.Context, req TrendRequest) ([]TrendBucket, error)

	// suggest
	Suggest(ctx context.Context, req SuggestRequest, teamID int64) (SuggestResponse, error)

	// detail
	GetTraceSummary(ctx context.Context, teamID int64, traceID string) (*TraceSummary, error)
	GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]SpanEvent, error)
	GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*SpanAttributes, error)
	GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error)
	ListSpansByTrace(ctx context.Context, teamID int64, traceID string) ([]SpanListItem, error)

	// paths
	GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]CriticalPathSpan, error)
	GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]ErrorPathSpan, error)

	// servicemap
	GetServiceMap(ctx context.Context, teamID int64, traceID string) (ServiceMapResponse, error)
	GetTraceErrors(ctx context.Context, teamID int64, traceID string) ([]TraceErrorGroup, error)
}

type TracesService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &TracesService{repo: repo}
}
