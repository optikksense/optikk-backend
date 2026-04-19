package explorer

import (
	spantraces "github.com/Optikk-Org/optikk-backend/internal/modules/traces/query"
)

// QueryRequest is the unified traces explorer request.
type QueryRequest struct {
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
	Query     string `json:"query"`
	Limit     int    `json:"limit"`
	Cursor    string `json:"cursor"`
	Step      string `json:"step"`
	OrderBy   string `json:"orderBy,omitempty"`
	OrderDir  string `json:"orderDir,omitempty"`
}

type FacetBucket struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
}

type PageInfo struct {
	HasMore    bool   `json:"hasMore"`
	NextCursor string `json:"nextCursor,omitempty"`
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
	SpanKind      []FacetBucket `json:"span_kind,omitempty"`
	HTTPMethod    []FacetBucket `json:"http_method,omitempty"`
	HTTPStatus    []FacetBucket `json:"http_status_code,omitempty"`
	DBSystem      []FacetBucket `json:"db_system,omitempty"`
}
