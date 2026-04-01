package explorer

import (
	logshared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"

	exploreranalytics "github.com/Optikk-Org/optikk-backend/internal/modules/explorer/analytics"
)

// QueryRequest is the new unified explorer request.
type QueryRequest struct {
	StartTime    int64                           `json:"startTime"`
	EndTime      int64                           `json:"endTime"`
	Query        string                          `json:"query"`
	Limit        int                             `json:"limit"`
	Offset       int                             `json:"offset"`
	Cursor       string                          `json:"cursor"`
	Direction    string                          `json:"direction"`
	Step         string                          `json:"step"`
	GroupBy      []string                        `json:"groupBy,omitempty"`
	Aggregations []exploreranalytics.Aggregation `json:"aggregations,omitempty"`
	VizMode      string                          `json:"vizMode,omitempty"`
	OrderBy      string                          `json:"orderBy,omitempty"`
	OrderDir     string                          `json:"orderDir,omitempty"`
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

// Response is returned when vizMode is empty or "list".
type Response struct {
	Results      []logshared.Log      `json:"results"`
	Summary      Summary              `json:"summary"`
	Facets       ExplorerFacets       `json:"facets"`
	Trend        LogVolumeData        `json:"trend"`
	PageInfo     PageInfo             `json:"pageInfo"`
	Correlations ExplorerCorrelations `json:"correlations,omitempty"`
}

type ExplorerFacets struct {
	Level       []Facet `json:"level"`
	ServiceName []Facet `json:"service_name"`
	Host        []Facet `json:"host,omitempty"`
	Pod         []Facet `json:"pod,omitempty"`
	Container   []Facet `json:"container,omitempty"`
	Environment []Facet `json:"environment,omitempty"`
	ScopeName   []Facet `json:"scope_name,omitempty"`
}

type ExplorerCorrelations struct {
	ServiceErrorRate LogAggregateResponse `json:"serviceErrorRate,omitempty"`
}

// AnalyticsResponse wraps the unified analytics result for non-list viz modes.
type AnalyticsResponse struct {
	*exploreranalytics.AnalyticsResult
}
