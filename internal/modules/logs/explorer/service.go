package explorer

import (
	"context"
	"fmt"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

// Service orchestrates POST /api/v1/logs/query. It owns the list path.
type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	limit := models.PickLimit(req.Limit, 50, 500)
	cur, _ := models.DecodeCursor(req.Cursor)
	
	rows, hasMore, err := s.repo.getLogs(ctx, req.Filters, limit, cur)
	if err != nil {
		return QueryResponse{}, fmt.Errorf("logs.Query.list: %w", err)
	}
	
	return QueryResponse{
		Results:  models.MapLogs(rows),
		PageInfo: buildPageInfo(rows, hasMore, limit),
	}, nil
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
