package consumer

import (
	"cmp"
	"context"
	"slices"
	"strconv"
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

// ----------------------------------------------------------------------------
// Rate panels — counter sum per (bucket, dim) divided by bucket-grain seconds.
// ----------------------------------------------------------------------------

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

func (s *Service) GetConsumeRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupRatePoint, error) {
	rows, err := s.repo.QueryConsumeRateByGroup(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := filter.FoldCounterRateByDim(rows,
		func(r GroupCounterRow) time.Time { return r.Timestamp },
		func(r GroupCounterRow) string { return r.ConsumerGroup },
		func(r GroupCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]GroupRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = GroupRatePoint{Timestamp: filter.FormatTime(fld.Ts), ConsumerGroup: fld.Dim, RatePerSec: fld.Rate}
	}
	return out, nil
}

func (s *Service) GetProcessRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupRatePoint, error) {
	rows, err := s.repo.QueryProcessRateByGroup(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := filter.FoldCounterRateByDim(rows,
		func(r GroupCounterRow) time.Time { return r.Timestamp },
		func(r GroupCounterRow) string { return r.ConsumerGroup },
		func(r GroupCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]GroupRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = GroupRatePoint{Timestamp: filter.FormatTime(fld.Ts), ConsumerGroup: fld.Dim, RatePerSec: fld.Rate}
	}
	return out, nil
}

// ----------------------------------------------------------------------------
// Latency panels — percentiles merged server-side via
// quantilesPrometheusHistogramMerge on metrics_1m.latency_state.
// ----------------------------------------------------------------------------

func (s *Service) GetReceiveLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicLatencyPoint, error) {
	rows, err := s.repo.QueryReceiveLatencyByTopic(ctx, teamID, startMs, endMs)
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

func (s *Service) GetProcessLatencyByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupLatencyPoint, error) {
	rows, err := s.repo.QueryProcessLatencyByGroup(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]GroupLatencyPoint, len(rows))
	for i, r := range rows {
		out[i] = GroupLatencyPoint{
			Timestamp:     filter.FormatTime(r.Timestamp),
			ConsumerGroup: r.ConsumerGroup,
			P50Ms:         filter.SecondsToMs(r.P50),
			P95Ms:         filter.SecondsToMs(r.P95),
			P99Ms:         filter.SecondsToMs(r.P99),
		}
	}
	return out, nil
}

// ----------------------------------------------------------------------------
// Error panels — counter sums per (bucket, group, error_type) / bucket secs.
// ----------------------------------------------------------------------------

func (s *Service) GetConsumeErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	rows, err := s.repo.QueryReceiveErrorsByGroup(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := filter.FoldErrorRateByPair(rows,
		func(r GroupErrorCounterRow) time.Time { return r.Timestamp },
		func(r GroupErrorCounterRow) string { return r.ConsumerGroup },
		func(r GroupErrorCounterRow) string { return r.ErrorType },
		func(r GroupErrorCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]ErrorRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = ErrorRatePoint{Timestamp: filter.FormatTime(fld.Ts), ConsumerGroup: fld.Dim, ErrorType: fld.ErrorType, ErrorRate: fld.Rate}
	}
	return out, nil
}

func (s *Service) GetProcessErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	rows, err := s.repo.QueryProcessErrorsByGroup(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := filter.FoldErrorRateByPair(rows,
		func(r GroupErrorCounterRow) time.Time { return r.Timestamp },
		func(r GroupErrorCounterRow) string { return r.ConsumerGroup },
		func(r GroupErrorCounterRow) string { return r.ErrorType },
		func(r GroupErrorCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]ErrorRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = ErrorRatePoint{Timestamp: filter.FormatTime(fld.Ts), ConsumerGroup: fld.Dim, ErrorType: fld.ErrorType, ErrorRate: fld.Rate}
	}
	return out, nil
}

// ----------------------------------------------------------------------------
// Lag panels.
// ----------------------------------------------------------------------------

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

func (s *Service) GetConsumerLagPerPartition(ctx context.Context, teamID int64, startMs, endMs int64) ([]PartitionLag, error) {
	rows, err := s.repo.QueryPartitionLagSnapshot(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]PartitionLag, 0, len(rows))
	for _, r := range rows {
		partition, _ := strconv.ParseInt(r.Partition, 10, 64)
		out = append(out, PartitionLag{
			Topic:         r.Topic,
			Partition:     partition,
			ConsumerGroup: r.ConsumerGroup,
			Lag:           int64(r.Lag),
		})
	}
	return out, nil
}

// ----------------------------------------------------------------------------
// Rebalance signals — 1 query, 6 metrics, fold per (bucket, group).
// ----------------------------------------------------------------------------

func (s *Service) GetRebalanceSignals(ctx context.Context, teamID int64, startMs, endMs int64) ([]RebalancePoint, error) {
	rows, err := s.repo.QueryRebalanceSignals(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type key struct {
		ts    time.Time
		group string
	}
	type acc struct {
		rebalance, join, sync, heartbeat, failed float64
		assignedSum                              float64
		assignedCount                            int
	}
	agg := map[key]*acc{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), group: r.ConsumerGroup}
		x, ok := agg[k]
		if !ok {
			x = &acc{}
			agg[k] = x
		}
		switch r.MetricName {
		case filter.MetricRebalanceCount:
			x.rebalance += r.Value
		case filter.MetricJoinCount:
			x.join += r.Value
		case filter.MetricSyncCount:
			x.sync += r.Value
		case filter.MetricHeartbeatCount:
			x.heartbeat += r.Value
		case filter.MetricFailedHeartbeatCount:
			x.failed += r.Value
		case filter.MetricAssignedPartitions:
			x.assignedSum += r.Value
			x.assignedCount++
		}
	}
	bucketSecs := timebucket.DisplayGrain(windowMs).Seconds()
	if bucketSecs <= 0 {
		bucketSecs = 1
	}
	out := make([]RebalancePoint, 0, len(agg))
	for k, x := range agg {
		var assigned float64
		if x.assignedCount > 0 {
			assigned = x.assignedSum / float64(x.assignedCount)
		}
		out = append(out, RebalancePoint{
			Timestamp:           filter.FormatTime(k.ts),
			ConsumerGroup:       k.group,
			RebalanceRate:       x.rebalance / bucketSecs,
			JoinRate:            x.join / bucketSecs,
			SyncRate:            x.sync / bucketSecs,
			HeartbeatRate:       x.heartbeat / bucketSecs,
			FailedHeartbeatRate: x.failed / bucketSecs,
			AssignedPartitions:  assigned,
		})
	}
	slices.SortFunc(out, func(a, b RebalancePoint) int {
		if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.ConsumerGroup, b.ConsumerGroup)
	})
	return out, nil
}
