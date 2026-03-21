package explorer

import (
	loganalytics "github.com/observability/observability-backend-go/internal/modules/log/analytics"
	logshared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"
)

type QueryRequest struct {
	StartTime int64          `json:"startTime"`
	EndTime   int64          `json:"endTime"`
	Limit     int            `json:"limit"`
	Offset    int            `json:"offset"`
	Cursor    string         `json:"cursor"`
	Direction string         `json:"direction"`
	Step      string         `json:"step"`
	Params    map[string]any `json:"params"`
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
	Results      []logshared.Log                `json:"results"`
	Summary      Summary                        `json:"summary"`
	Facets       map[string][]loganalytics.Facet `json:"facets"`
	Trend        loganalytics.LogVolumeData     `json:"trend"`
	PageInfo     PageInfo                       `json:"pageInfo"`
	Correlations map[string]any                 `json:"correlations,omitempty"`
}
