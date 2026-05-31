package traces

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type Repository interface {
	// explorer
	Query(ctx context.Context, req QueryRequest) ([]traceIndexRowDTO, bool, error)

	// facets
	QueryFacets(ctx context.Context, req FacetsRequest) (Facets, error)

	// trend
	QueryTrend(ctx context.Context, req TrendRequest) ([]TrendBucket, error)

	// suggest
	SuggestAttribute(ctx context.Context, teamID, startMs, endMs int64, attrKey, prefix string, limit int) ([]Suggestion, error)
	SuggestScalar(ctx context.Context, teamID, startMs, endMs int64, field, prefix string, limit int) ([]Suggestion, error)

	// detail
	GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]spanEventCombinedRow, error)
	GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*spanAttributeRow, error)
	GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error)
	GetTraceSummary(ctx context.Context, teamID int64, traceID string) (*TraceSummary, error)
	ListSpansByTrace(ctx context.Context, teamID int64, traceID string) ([]SpanListItem, error)

	// paths
	GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]criticalPathRow, error)
	GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]errorPathRow, error)

	// servicemap
	GetServiceMapSpans(ctx context.Context, teamID int64, traceID string) ([]serviceMapSpanRow, error)
	GetTraceErrors(ctx context.Context, teamID int64, traceID string) ([]traceErrorRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

