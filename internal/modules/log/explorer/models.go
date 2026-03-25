package explorer

import (
	loganalytics "github.com/observability/observability-backend-go/internal/modules/log/analytics"
	logshared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"
)

type LogExplorerParams struct {
	Search            string                       `json:"search,omitempty"`
	SearchMode        string                       `json:"searchMode,omitempty"`
	Severities        []string                     `json:"severities,omitempty"`
	ExcludeSeverities []string                     `json:"excludeSeverities,omitempty"`
	Services          []string                     `json:"services,omitempty"`
	ExcludeServices   []string                     `json:"excludeServices,omitempty"`
	Hosts             []string                     `json:"hosts,omitempty"`
	ExcludeHosts      []string                     `json:"excludeHosts,omitempty"`
	Pods              []string                     `json:"pods,omitempty"`
	Containers        []string                     `json:"containers,omitempty"`
	Environments      []string                     `json:"environments,omitempty"`
	Loggers           []string                     `json:"loggers,omitempty"`
	TraceID           string                       `json:"traceId,omitempty"`
	SpanID            string                       `json:"spanId,omitempty"`
	AttributeFilters  []logshared.LogAttributeFilter `json:"attributeFilters,omitempty"`
}

type QueryRequest struct {
	StartTime int64          `json:"startTime"`
	EndTime   int64          `json:"endTime"`
	Limit     int            `json:"limit"`
	Offset    int            `json:"offset"`
	Cursor    string         `json:"cursor"`
	Direction string         `json:"direction"`
	Step      string         `json:"step"`
	Params    LogExplorerParams `json:"params"`
}

type Summary struct {
	TotalLogs  int64 `json:"total_logs"`
	ErrorLogs  int64 `json:"error_logs"`
	WarnLogs   int64 `json:"warn_logs"`
	ServiceCnt int   `json:"service_count"`
}

type PageInfo struct {
	Total      int64  `json:"total"`
	HasMore    bool   `json:"hasMore"`
	NextCursor string `json:"nextCursor,omitempty"`
	Offset     int    `json:"offset"`
	Limit      int    `json:"limit"`
}

type Response struct {
	Results      []logshared.Log         `json:"results"`
	Summary      Summary                 `json:"summary"`
	Facets       ExplorerFacets          `json:"facets"`
	Trend        loganalytics.LogVolumeData `json:"trend"`
	PageInfo     PageInfo                `json:"pageInfo"`
	Correlations ExplorerCorrelations    `json:"correlations,omitempty"`
}

type ExplorerFacets struct {
	Level       []loganalytics.Facet `json:"level"`
	ServiceName []loganalytics.Facet `json:"service_name"`
	Host        []loganalytics.Facet `json:"host,omitempty"`
	Pod         []loganalytics.Facet `json:"pod,omitempty"`
	ScopeName   []loganalytics.Facet `json:"scope_name,omitempty"`
}

type ExplorerCorrelations struct {
	ServiceErrorRate loganalytics.LogAggregateResponse `json:"serviceErrorRate,omitempty"`
}
