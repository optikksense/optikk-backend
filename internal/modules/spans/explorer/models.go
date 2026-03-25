package explorer

import (
	spanlivetail "github.com/observability/observability-backend-go/internal/modules/spans/livetail"
	spantraces "github.com/observability/observability-backend-go/internal/modules/spans/traces"
)

type TraceExplorerParams struct {
	Services       []string `json:"services,omitempty"`
	Status         string   `json:"status,omitempty"`
	Search         string   `json:"search,omitempty"`
	MinDurationMs  *float64 `json:"minDuration,omitempty"`
	MaxDurationMs  *float64 `json:"maxDuration,omitempty"`
	TraceID        string   `json:"traceId,omitempty"`
	OperationName  string   `json:"operationName,omitempty"`
	HTTPMethod     string   `json:"httpMethod,omitempty"`
	HTTPStatusCode string   `json:"httpStatusCode,omitempty"`
	Mode           string   `json:"mode,omitempty"`
	SpanKind       string   `json:"spanKind,omitempty"`
	SpanName       string   `json:"spanName,omitempty"`
}

type QueryRequest struct {
	StartTime int64          `json:"startTime"`
	EndTime   int64          `json:"endTime"`
	Limit     int            `json:"limit"`
	Offset    int            `json:"offset"`
	Cursor    string         `json:"cursor"`
	Step      string         `json:"step"`
	Params    TraceExplorerParams `json:"params"`
}

type FacetBucket struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

type PageInfo struct {
	Total      int64  `json:"total"`
	HasMore    bool   `json:"hasMore"`
	NextCursor string `json:"nextCursor,omitempty"`
	Offset     int    `json:"offset"`
	Limit      int    `json:"limit"`
}

type Correlations struct {
	TopServices   []FacetBucket `json:"topServices,omitempty"`
	TopOperations []FacetBucket `json:"topOperations,omitempty"`
}

type Response struct {
	Results      []spantraces.Trace            `json:"results"`
	Summary      spantraces.TraceSummary       `json:"summary"`
	Facets       ExplorerFacets                `json:"facets"`
	Trend        []spantraces.TraceTrendBucket `json:"trend"`
	PageInfo     PageInfo                      `json:"pageInfo"`
	Correlations Correlations                  `json:"correlations,omitempty"`
}

type ExplorerFacets struct {
	ServiceName   []FacetBucket `json:"service_name"`
	Status        []FacetBucket `json:"status"`
	OperationName []FacetBucket `json:"operation_name"`
}

type StreamItem struct {
	Item         spanlivetail.LiveSpan `json:"item"`
	LagMs        int64                 `json:"lagMs"`
	DroppedCount int                   `json:"droppedCount"`
}
