package explorer

import (
	"context"
	"fmt"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
)

// Service orchestrates logs explorer read paths. Thin wrapper over the
// repository + its split siblings (repo_facets.go, repo_analytics.go,
// repo_volume.go). All CH reads go through the querycompiler seam.
type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// Query is the list-first path. `include` opt-ins unlock facets / trend /
// summary blocks so page-turns stay fast by default.
func (s *Service) Query(ctx context.Context, req QueryRequest, teamID int64) (QueryResponse, error) {
	filters, err := querycompiler.FromStructured(req.Filters, teamID, req.StartTime, req.EndTime)
	if err != nil {
		return QueryResponse{}, fmt.Errorf("logs.Query.parse: %w", err)
	}
	limit := pickLimit(req.Limit, 50, 500)
	cur, _ := DecodeCursor(req.Cursor)

	rows, hasMore, err := s.repo.ListLogs(ctx, filters, limit, cur)
	if err != nil {
		return QueryResponse{}, fmt.Errorf("logs.Query.list: %w", err)
	}

	resp := QueryResponse{
		Results:  mapLogs(rows),
		PageInfo: buildPageInfo(rows, hasMore, limit),
	}
	if err := s.applyIncludes(ctx, &resp, req.Include, filters); err != nil {
		return QueryResponse{}, err
	}
	return resp, nil
}

// applyIncludes attaches optional response blocks (facets/trend/summary).
// Compile warnings from the rollup path bubble up into resp.Warnings.
func (s *Service) applyIncludes(ctx context.Context, resp *QueryResponse, include []string, f querycompiler.Filters) error {
	want := toSet(include)
	if want["summary"] {
		summary, err := s.repo.Summary(ctx, f)
		if err != nil {
			return fmt.Errorf("logs.Query.summary: %w", err)
		}
		resp.Summary = &summary
	}
	if want["facets"] {
		facets, warns, err := s.repo.Facets(ctx, f)
		if err != nil {
			return fmt.Errorf("logs.Query.facets: %w", err)
		}
		resp.Facets = &facets
		resp.Warnings = append(resp.Warnings, warns...)
	}
	if want["trend"] {
		trend, warns, err := s.repo.Trend(ctx, f, "")
		if err != nil {
			return fmt.Errorf("logs.Query.trend: %w", err)
		}
		resp.Trend = trend
		resp.Warnings = append(resp.Warnings, warns...)
	}
	return nil
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

func mapLog(d logRowDTO) Log {
	return Log{
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
