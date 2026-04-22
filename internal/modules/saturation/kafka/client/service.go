package client

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka/filter"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// GetE2ELatency — 1 query, 3 metrics. Query already GROUPs BY (display_bucket,
// topic, metric_name) SQL-side, so each row carries the merged percentiles for
// one of the 3 e2e metrics. Service collates the 3 metric_name rows into a
// single E2ELatencyPoint per (display_bucket, topic).
func (s *Service) GetE2ELatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]E2ELatencyPoint, error) {
	rows, err := s.repo.QueryE2ELatencyByTopic(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type key struct {
		ts    time.Time
		topic string
	}
	type triple struct {
		publishP95, receiveP95, processP95 float64
	}
	acc := map[key]*triple{}
	for _, r := range rows {
		k := key{ts: r.Timestamp, topic: r.Topic}
		t, ok := acc[k]
		if !ok {
			t = &triple{}
			acc[k] = t
		}
		p95 := filter.SecondsToMs(r.P95)
		switch r.MetricName {
		case filter.MetricPublishDuration:
			t.publishP95 = p95
		case filter.MetricReceiveDuration:
			t.receiveP95 = p95
		case filter.MetricProcessDuration:
			t.processP95 = p95
		}
	}
	out := make([]E2ELatencyPoint, 0, len(acc))
	for k, t := range acc {
		out = append(out, E2ELatencyPoint{
			Timestamp:    filter.FormatTime(k.ts),
			Topic:        k.topic,
			PublishP95Ms: t.publishP95,
			ReceiveP95Ms: t.receiveP95,
			ProcessP95Ms: t.processP95,
		})
	}
	slices.SortFunc(out, func(a, b E2ELatencyPoint) int {
		if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.Topic, b.Topic)
	})
	return out, nil
}
