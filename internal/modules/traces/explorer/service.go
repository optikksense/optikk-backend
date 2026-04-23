package explorer

import (
	"context"
	"fmt"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
)

// Service orchestrates traces explorer read paths. List + facets + trend
// read traces_index; analytics also reads traces_index (raw-span fallback
// arrives with later features).
type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

func (s *Service) Query(ctx context.Context, req QueryRequest, teamID int64) (QueryResponse, error) {
	filters, err := querycompiler.FromStructured(req.Filters, teamID, req.StartTime, req.EndTime)
	if err != nil {
		return QueryResponse{}, fmt.Errorf("traces.Query.parse: %w", err)
	}
	limit := pickLimit(req.Limit, 50, 500)
	cur, _ := DecodeCursor(req.Cursor)

	rows, hasMore, warns, err := s.repo.ListTraces(ctx, filters, limit, cur)
	if err != nil {
		return QueryResponse{}, fmt.Errorf("traces.Query.list: %w", err)
	}

	resp := QueryResponse{
		Results:  mapTraces(rows),
		PageInfo: buildPageInfo(rows, hasMore, limit),
		Warnings: warns,
	}
	if err := s.applyIncludes(ctx, &resp, req.Include, filters); err != nil {
		return QueryResponse{}, err
	}
	return resp, nil
}

func (s *Service) applyIncludes(ctx context.Context, resp *QueryResponse, include []string, f querycompiler.Filters) error {
	want := toSet(include)
	if want["summary"] {
		summary, err := s.repo.Summarize(ctx, f)
		if err != nil {
			return fmt.Errorf("traces.Query.summary: %w", err)
		}
		resp.Summary = &summary
	}
	if want["facets"] {
		facets, err := s.repo.Facets(ctx, f)
		if err != nil {
			return fmt.Errorf("traces.Query.facets: %w", err)
		}
		resp.Facets = &facets
	}
	if want["trend"] {
		trend, err := s.repo.Trend(ctx, f)
		if err != nil {
			return fmt.Errorf("traces.Query.trend: %w", err)
		}
		resp.Trend = trend
	}
	return nil
}

func (s *Service) Analytics(ctx context.Context, req AnalyticsRequest, teamID int64) (AnalyticsResponse, error) {
	filters, err := querycompiler.FromStructured(req.Filters, teamID, req.StartTime, req.EndTime)
	if err != nil {
		return AnalyticsResponse{}, fmt.Errorf("traces.Analytics.parse: %w", err)
	}
	rows, warns, err := s.repo.Analytics(ctx, req, filters)
	if err != nil {
		return AnalyticsResponse{}, fmt.Errorf("traces.Analytics.query: %w", err)
	}
	return AnalyticsResponse{
		VizMode:  normalizeViz(req.VizMode),
		Step:     req.Step,
		Rows:     rows,
		Warnings: warns,
	}, nil
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

func normalizeViz(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "topn", "table", "pie":
		return strings.ToLower(v)
	default:
		return "timeseries"
	}
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
		info.NextCursor = TraceCursor{StartMs: last.StartMs, TraceID: last.TraceID}.Encode()
	}
	return info
}

func mapTrace(d traceIndexRowDTO) Trace {
	return Trace{
		TraceID:        d.TraceID,
		StartMs:        d.StartMs,
		EndMs:          d.EndMs,
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
