package client

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

// GetKafkaSummaryStats — 5 repo calls composed into one scalar response.
func (s *Service) GetKafkaSummaryStats(ctx context.Context, teamID int64, startMs, endMs int64) (KafkaSummaryStats, error) {
	durationSecs := float64(endMs-startMs) / 1000.0
	if durationSecs <= 0 {
		durationSecs = 1.0
	}
	stats := KafkaSummaryStats{}

	publishCount, err := s.repo.QueryPublishMessageCount(ctx, teamID, startMs, endMs)
	if err != nil {
		return stats, err
	}
	receiveCount, err := s.repo.QueryReceiveMessageCount(ctx, teamID, startMs, endMs)
	if err != nil {
		return stats, err
	}
	maxLag, err := s.repo.QueryMaxConsumerLag(ctx, teamID, startMs, endMs)
	if err != nil {
		return stats, err
	}
	publishHist, err := s.repo.QueryPublishDurationHistogram(ctx, teamID, startMs, endMs)
	if err != nil {
		return stats, err
	}
	receiveHist, err := s.repo.QueryReceiveDurationHistogram(ctx, teamID, startMs, endMs)
	if err != nil {
		return stats, err
	}

	stats.PublishRatePerSec = publishCount.Sum / durationSecs
	stats.ReceiveRatePerSec = receiveCount.Sum / durationSecs
	stats.MaxLag = maxLag.Max
	stats.PublishP95Ms = filter.SecondsToMs(publishHist.P95)
	stats.ReceiveP95Ms = filter.SecondsToMs(receiveHist.P95)
	return stats, nil
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

// GetBrokerConnections — gauge avg per (bucket, broker).
func (s *Service) GetBrokerConnections(ctx context.Context, teamID int64, startMs, endMs int64) ([]BrokerConnectionPoint, error) {
	rows, err := s.repo.QueryBrokerConnectionsSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type key struct {
		ts     time.Time
		broker string
	}
	type avg struct{ sum, count float64 }
	acc := map[key]*avg{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), broker: r.Broker}
		x, ok := acc[k]
		if !ok {
			x = &avg{}
			acc[k] = x
		}
		x.sum += r.Value
		x.count++
	}
	out := make([]BrokerConnectionPoint, 0, len(acc))
	for k, x := range acc {
		var v float64
		if x.count > 0 {
			v = x.sum / x.count
		}
		out = append(out, BrokerConnectionPoint{
			Timestamp:   filter.FormatTime(k.ts),
			Broker:      k.broker,
			Connections: v,
		})
	}
	slices.SortFunc(out, func(a, b BrokerConnectionPoint) int {
		if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.Broker, b.Broker)
	})
	return out, nil
}

func (s *Service) GetClientOperationDuration(ctx context.Context, teamID int64, startMs, endMs int64) ([]ClientOpDurationPoint, error) {
	rows, err := s.repo.QueryClientOperationDurationByOp(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]ClientOpDurationPoint, len(rows))
	for i, r := range rows {
		out[i] = ClientOpDurationPoint{
			Timestamp:     filter.FormatTime(r.Timestamp),
			OperationName: r.OperationName,
			P50Ms:         filter.SecondsToMs(r.P50),
			P95Ms:         filter.SecondsToMs(r.P95),
			P99Ms:         filter.SecondsToMs(r.P99),
		}
	}
	return out, nil
}

func (s *Service) GetClientOpErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	rows, err := s.repo.QueryClientOpErrorsByOperation(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := filter.FoldErrorRateByPair(rows,
		func(r OperationErrorCounterRow) time.Time { return r.Timestamp },
		func(r OperationErrorCounterRow) string { return r.OperationName },
		func(r OperationErrorCounterRow) string { return r.ErrorType },
		func(r OperationErrorCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]ErrorRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = ErrorRatePoint{Timestamp: filter.FormatTime(fld.Ts), OperationName: fld.Dim, ErrorType: fld.ErrorType, ErrorRate: fld.Rate}
	}
	return out, nil
}
