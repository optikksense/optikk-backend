package consumer

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka/filter"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// GetConsumeRateByTopic — counter sum per (bucket, topic) divided by bucket-grain seconds.
func (s *Service) GetConsumeRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicRatePoint, error) {
	rows, err := s.repo.QueryConsumeRateByTopic(ctx, teamID, startMs, endMs)
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

// GetConsumerLagByGroup — averages lag per (bucket, group, topic).
func (s *Service) GetConsumerLagByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]LagPoint, error) {
	rows, err := s.repo.QueryConsumerLagByGroupTopic(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type key struct {
		ts    time.Time
		group string
		topic string
	}
	type avg struct{ sum, count float64 }
	acc := map[key]*avg{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), group: r.ConsumerGroup, topic: r.Topic}
		x, ok := acc[k]
		if !ok {
			x = &avg{}
			acc[k] = x
		}
		x.sum += r.Value
		x.count++
	}
	out := make([]LagPoint, 0, len(acc))
	for k, x := range acc {
		var lag float64
		if x.count > 0 {
			lag = x.sum / x.count
		}
		out = append(out, LagPoint{
			Timestamp:     filter.FormatTime(k.ts),
			ConsumerGroup: k.group,
			Topic:         k.topic,
			Lag:           lag,
		})
	}
	slices.SortFunc(out, func(a, b LagPoint) int {
		if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.ConsumerGroup, b.ConsumerGroup)
	})
	return out, nil
}
