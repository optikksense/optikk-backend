package span_query //nolint:revive,stylecheck

import (
	"context"
	"fmt"
)

type Service interface {
	Query(ctx context.Context, req SpansQueryRequest) (SpansQueryResponse, error)
}

type service struct {
	repo Repository
}

func NewService(repo Repository) Service { return &service{repo: repo} }

func (s *service) Query(ctx context.Context, req SpansQueryRequest) (SpansQueryResponse, error) {
	limit := pickLimit(req.Limit, 50, 500)
	cur, _ := DecodeSpanCursor(req.Cursor)
	rows, hasMore, err := s.repo.ListSpans(ctx, req.Filters, limit, cur)
	if err != nil {
		return SpansQueryResponse{}, fmt.Errorf("span_query.list: %w", err)
	}
	return SpansQueryResponse{
		Results:  mapSpans(rows),
		PageInfo: buildPageInfo(rows, hasMore, limit),
	}, nil
}

func buildPageInfo(rows []spanRowDTO, hasMore bool, limit int) PageInfo {
	info := PageInfo{HasMore: hasMore, Limit: limit}
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		info.NextCursor = SpanCursor{TimestampNs: last.TimestampNs, SpanID: last.SpanID}.Encode()
	}
	return info
}

func mapSpan(d spanRowDTO) Span {
	return Span{
		SpanID:             d.SpanID,
		TraceID:            d.TraceID,
		ParentSpanID:       d.ParentSpanID,
		ServiceName:        d.ServiceName,
		Operation:          d.Operation,
		Kind:               d.Kind,
		DurationMs:         float64(d.DurationNano) / 1_000_000,
		TimestampNs:        d.TimestampNs,
		HasError:           d.HasError,
		Status:             d.Status,
		HTTPMethod:         d.HTTPMethod,
		ResponseStatusCode: d.ResponseStatusCode,
		Environment:        d.Environment,
	}
}

func mapSpans(rows []spanRowDTO) []Span {
	out := make([]Span, len(rows))
	for i, r := range rows {
		out[i] = mapSpan(r)
	}
	return out
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
