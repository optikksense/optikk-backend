package explorer

import (
	"context"
)

// Service orchestrates the traces list read path. Facets and trend live at
// peer endpoints (facets, trend) — the FE fans out three parallel
// requests; the server does not compose them.
type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

func (s *Service) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	limit := pickLimit(req.Limit, 50, 500)
	cur, _ := DecodeCursor(req.Cursor)
	rows, hasMore, err := s.repo.ListTraces(ctx, req.Filters, limit, cur)
	if err != nil {
		return QueryResponse{}, err
	}
	return QueryResponse{
		Results:  mapTraces(rows),
		PageInfo: buildPageInfo(rows, hasMore, limit),
	}, nil
}

func pickLimit(v, def, maxLimit int) int {
	if v <= 0 {
		return def
	}
	if v > maxLimit {
		return maxLimit
	}
	return v
}

func buildPageInfo(rows []traceIndexRowDTO, hasMore bool, limit int) PageInfo {
	info := PageInfo{HasMore: hasMore, Limit: limit}
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		info.NextCursor = TraceCursor{StartMs: uint64(last.StartTime.UnixMilli()), TraceID: last.TraceID}.Encode()
	}
	return info
}

func mapTrace(d traceIndexRowDTO) Trace {
	return Trace{
		TraceID:        d.TraceID,
		StartMs:        uint64(d.StartTime.UnixMilli()),
		EndMs:          uint64(d.EndTime.UnixMilli()),
		DurationMs:     float64(d.DurationNs) / 1_000_000,
		RootService:    d.RootService,
		RootOperation:  d.RootOperation,
		RootStatus:     d.RootStatus,
		RootHTTPMethod: d.RootHTTPMethod,
		RootHTTPStatus: d.RootHTTPStatus,
		SpanCount:      uint32(d.SpanCount),
		HasError:       d.HasError,
		ErrorCount:     uint32(d.ErrorCount),
		ServiceSet:     d.ServiceSet,
		Truncated:      d.Truncated,
	}
}

func mapTraces(rows []traceIndexRowDTO) []Trace {
	out := make([]Trace, len(rows))
	for i, r := range rows {
		out[i] = mapTrace(r)
	}
	return out
}
