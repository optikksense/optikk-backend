package explorer

import (
	"context"
	"fmt"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
	"golang.org/x/sync/errgroup"
)

// Service orchestrates traces explorer read paths. List + facets + trend are
// fanned out in parallel via errgroup so the response = max of all branches.
type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

type queryParts struct {
	rows    []traceIndexRowDTO
	hasMore bool
	facets  *Facets
	trend   []TrendBucket
}

func (s *Service) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	limit := pickLimit(req.Limit, 50, 500)
	cur, _ := DecodeCursor(req.Cursor)
	parts, err := s.fetchQueryParts(ctx, req.Filters, limit, cur, toSet(req.Include))
	if err != nil {
		return QueryResponse{}, err
	}
	return QueryResponse{
		Results:  mapTraces(parts.rows),
		PageInfo: buildPageInfo(parts.rows, parts.hasMore, limit),
		Facets:   parts.facets,
		Trend:    parts.trend,
	}, nil
}

func (s *Service) fetchQueryParts(
	ctx context.Context, f filter.Filters, limit int, cur TraceCursor, want map[string]bool,
) (queryParts, error) {
	var p queryParts
	g, gctx := errgroup.WithContext(ctx)
	g.Go(s.listJob(gctx, f, limit, cur, &p))
	if want["facets"] {
		g.Go(s.facetsJob(gctx, f, &p))
	}
	if want["trend"] {
		g.Go(s.trendJob(gctx, f, &p))
	}
	if err := g.Wait(); err != nil {
		return queryParts{}, err
	}
	return p, nil
}

func (s *Service) listJob(ctx context.Context, f filter.Filters, limit int, cur TraceCursor, p *queryParts) func() error {
	return func() error {
		r, hm, err := s.repo.ListTraces(ctx, f, limit, cur)
		if err != nil {
			return fmt.Errorf("traces.Query.list: %w", err)
		}
		p.rows, p.hasMore = r, hm
		return nil
	}
}

func (s *Service) facetsJob(ctx context.Context, f filter.Filters, p *queryParts) func() error {
	return func() error {
		fc, err := s.repo.Facets(ctx, f)
		if err != nil {
			return fmt.Errorf("traces.Query.facets: %w", err)
		}
		p.facets = &fc
		return nil
	}
}

func (s *Service) trendJob(ctx context.Context, f filter.Filters, p *queryParts) func() error {
	return func() error {
		tr, err := s.repo.Trend(ctx, f)
		if err != nil {
			return fmt.Errorf("traces.Query.trend: %w", err)
		}
		p.trend = tr
		return nil
	}
}

func (s *Service) GetByID(ctx context.Context, teamID int64, traceID string) (*Trace, error) {
	row, err := s.repo.GetByID(ctx, teamID, traceID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	t := mapTrace(*row)
	return &t, nil
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

func toSet(items []string) map[string]bool {
	out := make(map[string]bool, len(items))
	for _, it := range items {
		out[strings.ToLower(strings.TrimSpace(it))] = true
	}
	return out
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
		SpanCount:      d.SpanCount,
		HasError:       d.HasError,
		ErrorCount:     d.ErrorCount,
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
