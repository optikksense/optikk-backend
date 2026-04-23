package explorer

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
	"golang.org/x/sync/errgroup"
)

// Service orchestrates logs explorer read paths. Thin wrapper over the
// repository + its split siblings (repo_facets.go, repo_analytics.go,
// repo_volume.go). All CH reads go through the querycompiler seam.
type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// queryParts carries the slots each parallel fetch fills. The list fetch
// is always run; summary/facets/trend slots stay empty when the caller
// didn't ask for them via req.Include. Jobs write disjoint slots so we
// don't need a lock — warnings are concatenated in Query() after Wait().
type queryParts struct {
	rows         []logRowDTO
	hasMore      bool
	summary      *Summary
	facets       *Facets
	trend        []TrendBucket
	facetsWarns  []string
	trendWarns   []string
}

// Query is the list-first path. `include` opt-ins unlock facets / trend /
// summary blocks. List + asked-for includes run in parallel via errgroup
// so the response = max(list, summary, facets, trend), not sum.
func (s *Service) Query(ctx context.Context, req QueryRequest, teamID int64) (QueryResponse, error) {
	filters, err := querycompiler.FromStructured(req.Filters, teamID, req.StartTime, req.EndTime)
	if err != nil {
		return QueryResponse{}, fmt.Errorf("logs.Query.parse: %w", err)
	}
	limit := pickLimit(req.Limit, 50, 500)
	cur, _ := DecodeCursor(req.Cursor)
	parts, err := s.fetchQueryParts(ctx, filters, limit, cur, toSet(req.Include))
	if err != nil {
		return QueryResponse{}, err
	}
	return QueryResponse{
		Results:  mapLogs(parts.rows),
		PageInfo: buildPageInfo(parts.rows, parts.hasMore, limit),
		Summary:  parts.summary,
		Facets:   parts.facets,
		Trend:    parts.trend,
		Warnings: append(parts.facetsWarns, parts.trendWarns...),
	}, nil
}

// fetchQueryParts fans list + summary + facets + trend out in parallel.
// Perf note: replaced four sequential ClickHouse reads with max-of-four.
func (s *Service) fetchQueryParts(
	ctx context.Context, f querycompiler.Filters, limit int, cur Cursor, want map[string]bool,
) (queryParts, error) {
	var p queryParts
	g, gctx := errgroup.WithContext(ctx)
	g.Go(s.listJob(gctx, f, limit, cur, &p))
	if want["summary"] {
		g.Go(s.summaryJob(gctx, f, &p))
	}
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

func (s *Service) listJob(ctx context.Context, f querycompiler.Filters, limit int, cur Cursor, p *queryParts) func() error {
	return func() error {
		rows, hasMore, err := s.repo.ListLogs(ctx, f, limit, cur)
		if err != nil {
			return fmt.Errorf("logs.Query.list: %w", err)
		}
		p.rows, p.hasMore = rows, hasMore
		return nil
	}
}

func (s *Service) summaryJob(ctx context.Context, f querycompiler.Filters, p *queryParts) func() error {
	return func() error {
		sm, err := s.repo.Summary(ctx, f)
		if err != nil {
			return fmt.Errorf("logs.Query.summary: %w", err)
		}
		p.summary = &sm
		return nil
	}
}

func (s *Service) facetsJob(ctx context.Context, f querycompiler.Filters, p *queryParts) func() error {
	return func() error {
		fc, warns, err := s.repo.Facets(ctx, f)
		if err != nil {
			return fmt.Errorf("logs.Query.facets: %w", err)
		}
		p.facets = &fc
		p.facetsWarns = warns
		return nil
	}
}

func (s *Service) trendJob(ctx context.Context, f querycompiler.Filters, p *queryParts) func() error {
	return func() error {
		tr, warns, err := s.repo.Trend(ctx, f, "")
		if err != nil {
			return fmt.Errorf("logs.Query.trend: %w", err)
		}
		p.trend = tr
		p.trendWarns = warns
		return nil
	}
}

// Analytics dispatches to rollup when filters allow, raw otherwise.
func (s *Service) Analytics(ctx context.Context, req AnalyticsRequest, teamID int64) (AnalyticsResponse, error) {
	filters, err := querycompiler.FromStructured(req.Filters, teamID, req.StartTime, req.EndTime)
	if err != nil {
		return AnalyticsResponse{}, fmt.Errorf("logs.Analytics.parse: %w", err)
	}
	rows, step, warns, err := s.repo.Analytics(ctx, filters, req)
	if err != nil {
		return AnalyticsResponse{}, fmt.Errorf("logs.Analytics.query: %w", err)
	}
	return AnalyticsResponse{
		VizMode:  normalizeViz(req.VizMode),
		Step:     step,
		Rows:     rows,
		Warnings: warns,
	}, nil
}

// GetByID reads a single log by the deep-link id. Returns nil on not-found.
func (s *Service) GetByID(ctx context.Context, teamID int64, id string) (*Log, error) {
	traceID, spanID, tsNs, ok := parseLogID(id)
	if !ok {
		return nil, fmt.Errorf("logs.GetByID: malformed id %q", id)
	}
	row, err := s.repo.GetByID(ctx, teamID, traceID, spanID, tsNs)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	log := mapLog(*row)
	return &log, nil
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
	for _, item := range items {
		out[strings.ToLower(strings.TrimSpace(item))] = true
	}
	return out
}

func buildPageInfo(rows []logRowDTO, hasMore bool, limit int) PageInfo {
	info := PageInfo{HasMore: hasMore, Limit: limit}
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		info.NextCursor = Cursor{
			Timestamp:         last.Timestamp,
			ObservedTimestamp: last.ObservedTimestamp,
			TraceID:           last.TraceID,
		}.Encode()
	}
	return info
}

// encodeLogListID matches GET /logs/:id decoding (trace_id:span_id:ts_ns as base64url).
func encodeLogListID(d logRowDTO) string {
	raw := fmt.Sprintf("%s:%s:%d", d.TraceID, d.SpanID, d.Timestamp.UnixNano())
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func mapLog(d logRowDTO) Log {
	return Log{
		ID:                encodeLogListID(d),
		Timestamp:         uint64(d.Timestamp.UnixNano()), //nolint:gosec // G115 domain-bounded
		ObservedTimestamp: d.ObservedTimestamp,
		SeverityText:      d.SeverityText,
		SeverityNumber:    d.SeverityNumber,
		SeverityBucket:    d.SeverityBucket,
		Body:              d.Body,
		TraceID:           d.TraceID,
		SpanID:            d.SpanID,
		TraceFlags:        d.TraceFlags,
		ServiceName:       d.ServiceName,
		Host:              d.Host,
		Pod:               d.Pod,
		Container:         d.Container,
		Environment:       d.Environment,
		AttributesString:  d.AttributesString,
		AttributesNumber:  d.AttributesNumber,
		AttributesBool:    d.AttributesBool,
		ScopeName:         d.ScopeName,
		ScopeVersion:      d.ScopeVersion,
	}
}

func mapLogs(rows []logRowDTO) []Log {
	out := make([]Log, len(rows))
	for i, r := range rows {
		out[i] = mapLog(r)
	}
	return out
}
