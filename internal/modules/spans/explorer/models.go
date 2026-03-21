package explorer

import spanlivetail "github.com/observability/observability-backend-go/internal/modules/spans/livetail"

type QueryRequest struct {
	StartTime int64          `json:"startTime"`
	EndTime   int64          `json:"endTime"`
	Limit     int            `json:"limit"`
	Offset    int            `json:"offset"`
	Cursor    string         `json:"cursor"`
	Step      string         `json:"step"`
	Params    map[string]any `json:"params"`
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

type Response struct {
	Results      any            `json:"results"`
	Summary      any            `json:"summary"`
	Facets       map[string][]FacetBucket `json:"facets"`
	Trend        any            `json:"trend"`
	PageInfo     PageInfo       `json:"pageInfo"`
	Correlations map[string]any `json:"correlations,omitempty"`
}

type StreamItem struct {
	Item         spanlivetail.LiveSpan `json:"item"`
	LagMs        int64                 `json:"lagMs"`
	DroppedCount int                   `json:"droppedCount"`
}
