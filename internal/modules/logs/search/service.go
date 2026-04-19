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
	rows, hasMore, err := s.repo.GetLogs(ctx, f, limit, direction, cursor)
	if err != nil {
		return LogSearchResponse{}, err
	}

	logs := shared.MapLogRows(rows)

	var nextCursor string
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		nextCursor = shared.LogCursor{
			Timestamp:         last.Timestamp,
			ObservedTimestamp: last.ObservedTimestamp,
			TraceID:           last.TraceID,
		}.Encode()
	}

	return LogSearchResponse{
		Logs:       logs,
		HasMore:    hasMore,
		NextCursor: nextCursor,
		Limit:      limit,
	}, nil
}
