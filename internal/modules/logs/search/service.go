package search

import (
	"context"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetLogs(ctx context.Context, f shared.LogFilters, limit int, direction string, cursor shared.LogCursor) (LogSearchResponse, error) {
	rows, total, err := s.repo.GetLogs(ctx, f, limit, direction, cursor)
	if err != nil {
		return LogSearchResponse{}, err
	}

	logs := shared.MapLogRows(rows)
	currentOffset := cursor.Offset
	if currentOffset < 0 {
		currentOffset = 0
	}
	nextOffset := currentOffset + len(logs)
	hasMore := int64(nextOffset) < total

	var nextCursor string
	if hasMore {
		nextCursor = shared.LogCursor{Offset: nextOffset}.Encode()
	}

	return LogSearchResponse{
		Logs:       logs,
		HasMore:    hasMore,
		NextCursor: nextCursor,
		Limit:      limit,
		Total:      total,
	}, nil
}
