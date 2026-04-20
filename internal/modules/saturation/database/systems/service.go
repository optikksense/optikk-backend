package systems

import (
	"context"
	"fmt"

	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
)

type Service struct {
	repo    Repository
	sketchQ *sketch.Querier //nolint:unused // reserved for future p95/p99 columns on the systems view
}

func NewService(repo Repository, sketchQ *sketch.Querier) *Service {
	return &Service{repo: repo, sketchQ: sketchQ}
}

// teamIDString converts the int64 tenant id to the string form used by all
// sketch keys. Kept on the service for parity with other sketch-backed
// modules; the systems view currently only needs avg, not percentiles.
//
//nolint:unused // parity helper for future sketch calls.
func teamIDString(teamID int64) string { return fmt.Sprintf("%d", teamID) }

func (s *Service) GetDetectedSystems(ctx context.Context, teamID int64, startMs, endMs int64) ([]DetectedSystem, error) {
	rows, err := s.repo.GetDetectedSystems(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	// Avg latency (ms) — sum/count in Go. hist_sum is in seconds, so * 1000.
	// Replaces the prior SQL `quantileTDigestWeighted(0.50) * 1000` which was
	// labelled "avg" but was actually a weighted median. Callers treat the
	// column as a representative latency, so sum/count is a closer fit.
	for i := range rows {
		if rows[i].LatencyCount > 0 {
			rows[i].AvgLatencyMs = (rows[i].LatencySum / float64(rows[i].LatencyCount)) * 1000.0
		}
	}
	return rows, nil
}
