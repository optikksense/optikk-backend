package producer

import (
	"context"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka/filter"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetProduceRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicRatePoint, error) {
	rows, err := s.repo.QueryPublishRateByTopic(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := filter.FoldCounterRateByDim(rows,
		func(r TopicCounterRow) time.Time { return r.Timestamp },
		func(r TopicCounterRow) string { return r.Topic },
		func(r TopicCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]TopicRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = TopicRatePoint{Timestamp: filter.FormatTime(fld.Ts), Topic: fld.Dim, RatePerSec: fld.Rate}
	}
	return out, nil
}
