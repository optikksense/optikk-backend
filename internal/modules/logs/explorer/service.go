package explorer

import (
	"context"
	"fmt"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
	"golang.org/x/sync/errgroup"
)

// FacetsClient is the in-process API explorer needs from log_facets to fan
// the optional facets include alongside the list query.
type FacetsClient interface {
	Compute(ctx context.Context, f filter.Filters) (models.Facets, error)
}

// TrendsClient is the in-process API explorer needs from log_trends to fan
// the optional summary/trend includes alongside the list query.
type TrendsClient interface {
	Summary(ctx context.Context, f filter.Filters) (models.Summary, error)
	Trend(ctx context.Context, f filter.Filters, step string) ([]models.TrendBucket, error)
}

// Service orchestrates POST /api/v1/logs/query. It owns the list path and
// fans summary/facets/trend includes out to sibling submodules in parallel
// via errgroup so the response = max(list, summary, facets, trend).
type Service struct {
	repo   *Repository
	facets FacetsClient
	trends TrendsClient
}

func NewService(repo *Repository, facets FacetsClient, trends TrendsClient) *Service {
	return &Service{repo: repo, facets: facets, trends: trends}
}

type queryParts struct {
	rows    []models.LogRow
	hasMore bool
	summary *models.Summary
	facets  *models.Facets
	trend   []models.TrendBucket
}

func (s *Service) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	limit := models.PickLimit(req.Limit, 50, 500)
	cur, _ := models.DecodeCursor(req.Cursor)
	parts, err := s.fetchQueryParts(ctx, req.Filters, limit, cur, toSet(req.Include))
	if err != nil {
		return QueryResponse{}, err
	}
	return QueryResponse{
		Results:  models.MapLogs(parts.rows),
		PageInfo: buildPageInfo(parts.rows, parts.hasMore, limit),
		Summary:  parts.summary,
		Facets:   parts.facets,
		Trend:    parts.trend,
	}, nil
}

func (s *Service) fetchQueryParts(
	ctx context.Context, f filter.Filters, limit int, cur models.Cursor, want map[string]bool,
) (queryParts, error) {
	var p queryParts
	g, gctx := errgroup.WithContext(ctx)
	g.Go(s.listJob(gctx, f, limit, cur, &p))
	if want["summary"] && s.trends != nil {
		g.Go(s.summaryJob(gctx, f, &p))
	}
	if want["facets"] && s.facets != nil {
		g.Go(s.facetsJob(gctx, f, &p))
	}
	if want["trend"] && s.trends != nil {
		g.Go(s.trendJob(gctx, f, &p))
	}
	if err := g.Wait(); err != nil {
		return queryParts{}, err
	}
	return p, nil
}

func (s *Service) listJob(ctx context.Context, f filter.Filters, limit int, cur models.Cursor, p *queryParts) func() error {
	return func() error {
		rows, hasMore, err := s.repo.ListLogs(ctx, f, limit, cur)
		if err != nil {
			return fmt.Errorf("logs.Query.list: %w", err)
		}
		p.rows, p.hasMore = rows, hasMore
		return nil
	}
}

func (s *Service) summaryJob(ctx context.Context, f filter.Filters, p *queryParts) func() error {
	return func() error {
		sm, err := s.trends.Summary(ctx, f)
		if err != nil {
			return fmt.Errorf("logs.Query.summary: %w", err)
		}
		p.summary = &sm
		return nil
	}
}

func (s *Service) facetsJob(ctx context.Context, f filter.Filters, p *queryParts) func() error {
	return func() error {
		fc, err := s.facets.Compute(ctx, f)
		if err != nil {
			return fmt.Errorf("logs.Query.facets: %w", err)
		}
		p.facets = &fc
		return nil
	}
}

func (s *Service) trendJob(ctx context.Context, f filter.Filters, p *queryParts) func() error {
	return func() error {
		tr, err := s.trends.Trend(ctx, f, "")
		if err != nil {
			return fmt.Errorf("logs.Query.trend: %w", err)
		}
		p.trend = tr
		return nil
	}
}

func toSet(items []string) map[string]bool {
	out := make(map[string]bool, len(items))
	for _, item := range items {
		out[strings.ToLower(strings.TrimSpace(item))] = true
	}
	return out
}

func buildPageInfo(rows []models.LogRow, hasMore bool, limit int) models.PageInfo {
	info := models.PageInfo{HasMore: hasMore, Limit: limit}
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		info.NextCursor = models.Cursor{
			Timestamp:         last.Timestamp,
			ObservedTimestamp: last.ObservedTimestamp,
			TraceID:           last.TraceID,
		}.Encode()
	}
	return info
}
