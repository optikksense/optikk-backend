package patterns

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	logshared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
)

type PatternResult struct {
	Pattern   string `json:"pattern"    ch:"pattern"`
	Count     int64  `json:"count"      ch:"count"`
	FirstSeen uint64 `json:"first_seen" ch:"first_seen"`
	LastSeen  uint64 `json:"last_seen"  ch:"last_seen"`
	Sample    string `json:"sample"     ch:"sample"`
}

type PatternsResponse struct {
	Patterns []PatternResult `json:"patterns"`
}

type Service struct {
	db *dbutil.NativeQuerier
}

func NewService(db *dbutil.NativeQuerier) *Service {
	return &Service{db: db}
}

func (s *Service) GetPatterns(ctx context.Context, f logshared.LogFilters, limit int) (PatternsResponse, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	where, args := logshared.BuildLogWhere(f)

	query := fmt.Sprintf(`
		SELECT
			replaceRegexpAll(
				replaceRegexpAll(
					replaceRegexpAll(
						replaceRegexpAll(body,
							'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '<UUID>'),
						'\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b', '<IP>'),
					'\\b[0-9a-f]{24,64}\\b', '<HEX>'),
				'\\b\\d+\\b', '<N>')
			AS pattern,
			count() AS count,
			min(timestamp) AS first_seen,
			max(timestamp) AS last_seen,
			any(body) AS sample
		FROM observability.logs
		WHERE %s
		GROUP BY pattern
		ORDER BY count DESC
		LIMIT %d`, where, limit)

	var rows []PatternResult
	if err := s.db.Select(ctx, &rows, query, args...); err != nil {
		return PatternsResponse{}, fmt.Errorf("patterns query failed: %w", err)
	}

	return PatternsResponse{Patterns: rows}, nil
}
