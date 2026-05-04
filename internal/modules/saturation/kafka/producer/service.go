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

// GetPublishLatencyByTopic — percentiles are merged server-side via
// quantilesPrometheusHistogramMerge on metrics_1m.latency_state; service is a
// thin per-row mapper that converts seconds → ms.
func (s *Service) GetPublishLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicLatencyPoint, error) {
	rows, err := s.repo.QueryPublishLatencyByTopic(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]TopicLatencyPoint, len(rows))
	for i, r := range rows {
		out[i] = TopicLatencyPoint{
			Timestamp: filter.FormatTime(r.Timestamp),
			Topic:     r.Topic,
			P50Ms:     filter.SecondsToMs(r.P50),
			P95Ms:     filter.SecondsToMs(r.P95),
			P99Ms:     filter.SecondsToMs(r.P99),
		}
	}
	return out, nil
}

func (s *Service) GetPublishErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	rows, err := s.repo.QueryPublishErrorsByTopic(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := filter.FoldErrorRateByPair(rows,
		func(r TopicErrorCounterRow) time.Time { return r.Timestamp },
		func(r TopicErrorCounterRow) string { return r.Topic },
		func(r TopicErrorCounterRow) string { return r.ErrorType },
		func(r TopicErrorCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]ErrorRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = ErrorRatePoint{Timestamp: filter.FormatTime(fld.Ts), Topic: fld.Dim, ErrorType: fld.ErrorType, ErrorRate: fld.Rate}
	}
	return out, nil
}
