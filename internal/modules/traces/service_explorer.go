package traces

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

const (
	defaultSuggestLimit = 10
	maxSuggestLimit     = 50
)

var scalarFields = map[string]struct{}{
	"service":     {},
	"operation":   {},
	"http_method": {},
	"http_status": {},
	"status":      {},
	"environment": {},
}

func IsScalarField(field string) bool {
	_, ok := scalarFields[field]
	return ok
}

func (s *TracesService) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	limit := pickExplorerLimit(req.Limit, 50, 500)
	rows, hasMore, err := s.repo.Query(ctx, req)
	if err != nil {
		return QueryResponse{}, err
	}
	return QueryResponse{
		Results:  mapTraces(rows),
		PageInfo: buildPageInfo(rows, hasMore, limit),
	}, nil
}

func pickExplorerLimit(v, def, maxLimit int) int {
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

func (s *TracesService) QueryFacets(ctx context.Context, req FacetsRequest) (Facets, error) {
	return s.repo.QueryFacets(ctx, req)
}

func (s *TracesService) QueryTrend(ctx context.Context, req TrendRequest) ([]TrendBucket, error) {
	return s.repo.QueryTrend(ctx, req)
}

func (s *TracesService) Suggest(ctx context.Context, req SuggestRequest, teamID int64) (SuggestResponse, error) {
	limit := pickSuggestLimit(req.Limit)
	rows, err := s.fetchSuggest(ctx, teamID, req, limit)
	if err != nil {
		slog.ErrorContext(ctx, "suggest: Suggest failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("field", req.Field))
		return SuggestResponse{}, err
	}
	return SuggestResponse{Suggestions: rows}, nil
}

func (s *TracesService) fetchSuggest(ctx context.Context, teamID int64, req SuggestRequest, limit int) ([]Suggestion, error) {
	if strings.HasPrefix(req.Field, "@") {
		return s.repo.SuggestAttribute(ctx, teamID, req.StartTime, req.EndTime, req.Field, req.Prefix, limit)
	}
	if !IsScalarField(req.Field) {
		return nil, fmt.Errorf("suggest: unknown scalar field %q", req.Field)
	}
	return s.repo.SuggestScalar(ctx, teamID, req.StartTime, req.EndTime, req.Field, req.Prefix, limit)
}

func pickSuggestLimit(v int) int {
	if v <= 0 {
		return defaultSuggestLimit
	}
	if v > maxSuggestLimit {
		return maxSuggestLimit
	}
	return v
}
