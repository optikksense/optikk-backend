package kafka

import (
	"cmp"
	"context"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// secondsToMs converts a seconds-domain percentile (as returned by
// metrics_1m.latency_state, since OTel kafka duration metrics are
// seconds-domain) into milliseconds.
func secondsToMs(s float64) float64 { return s * 1000.0 }

type KafkaService struct {
	repo *ClickHouseRepository
}

func NewService(repo *ClickHouseRepository) *KafkaService {
	return &KafkaService{repo: repo}
}

// Summary stats — 5 repo calls composed into one scalar response.

func (s *KafkaService) GetKafkaSummaryStats(ctx context.Context, teamID int64, startMs, endMs int64) (KafkaSummaryStats, error) {
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
	stats.PublishP95Ms = secondsToMs(publishHist.P95)
	stats.ReceiveP95Ms = secondsToMs(receiveHist.P95)
	return stats, nil
}

// Rate panels — counter sum per (bucket, dim) divided by bucket-grain seconds.

func (s *KafkaService) GetProduceRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicRatePoint, error) {
	rows, err := s.repo.QueryPublishRateByTopic(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := foldCounterRateByDim(rows,
		func(r TopicCounterRow) time.Time { return r.Timestamp },
		func(r TopicCounterRow) string { return r.Topic },
		func(r TopicCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]TopicRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = TopicRatePoint{Timestamp: formatTime(fld.ts), Topic: fld.dim, RatePerSec: fld.rate}
	}
	return out, nil
}

func (s *KafkaService) GetConsumeRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicRatePoint, error) {
	rows, err := s.repo.QueryConsumeRateByTopic(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := foldCounterRateByDim(rows,
		func(r TopicCounterRow) time.Time { return r.Timestamp },
		func(r TopicCounterRow) string { return r.Topic },
		func(r TopicCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]TopicRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = TopicRatePoint{Timestamp: formatTime(fld.ts), Topic: fld.dim, RatePerSec: fld.rate}
	}
	return out, nil
}

func (s *KafkaService) GetConsumeRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupRatePoint, error) {
	rows, err := s.repo.QueryConsumeRateByGroup(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := foldCounterRateByDim(rows,
		func(r GroupCounterRow) time.Time { return r.Timestamp },
		func(r GroupCounterRow) string { return r.ConsumerGroup },
		func(r GroupCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]GroupRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = GroupRatePoint{Timestamp: formatTime(fld.ts), ConsumerGroup: fld.dim, RatePerSec: fld.rate}
	}
	return out, nil
}

func (s *KafkaService) GetProcessRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupRatePoint, error) {
	rows, err := s.repo.QueryProcessRateByGroup(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := foldCounterRateByDim(rows,
		func(r GroupCounterRow) time.Time { return r.Timestamp },
		func(r GroupCounterRow) string { return r.ConsumerGroup },
		func(r GroupCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]GroupRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = GroupRatePoint{Timestamp: formatTime(fld.ts), ConsumerGroup: fld.dim, RatePerSec: fld.rate}
	}
	return out, nil
}

// Latency panels — percentiles are merged server-side via
// quantilesPrometheusHistogramMerge on metrics_1m.latency_state; service is a
// thin per-row mapper that converts seconds → ms.

func (s *KafkaService) GetPublishLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicLatencyPoint, error) {
	rows, err := s.repo.QueryPublishLatencyByTopic(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]TopicLatencyPoint, len(rows))
	for i, r := range rows {
		out[i] = TopicLatencyPoint{
			Timestamp: formatTime(r.Timestamp),
			Topic:     r.Topic,
			P50Ms:     secondsToMs(r.P50),
			P95Ms:     secondsToMs(r.P95),
			P99Ms:     secondsToMs(r.P99),
		}
	}
	return out, nil
}

func (s *KafkaService) GetReceiveLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicLatencyPoint, error) {
	rows, err := s.repo.QueryReceiveLatencyByTopic(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]TopicLatencyPoint, len(rows))
	for i, r := range rows {
		out[i] = TopicLatencyPoint{
			Timestamp: formatTime(r.Timestamp),
			Topic:     r.Topic,
			P50Ms:     secondsToMs(r.P50),
			P95Ms:     secondsToMs(r.P95),
			P99Ms:     secondsToMs(r.P99),
		}
	}
	return out, nil
}

func (s *KafkaService) GetProcessLatencyByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupLatencyPoint, error) {
	rows, err := s.repo.QueryProcessLatencyByGroup(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]GroupLatencyPoint, len(rows))
	for i, r := range rows {
		out[i] = GroupLatencyPoint{
			Timestamp:     formatTime(r.Timestamp),
			ConsumerGroup: r.ConsumerGroup,
			P50Ms:         secondsToMs(r.P50),
			P95Ms:         secondsToMs(r.P95),
			P99Ms:         secondsToMs(r.P99),
		}
	}
	return out, nil
}

func (s *KafkaService) GetClientOperationDuration(ctx context.Context, teamID int64, startMs, endMs int64) ([]ClientOpDurationPoint, error) {
	rows, err := s.repo.QueryClientOperationDurationByOp(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]ClientOpDurationPoint, len(rows))
	for i, r := range rows {
		out[i] = ClientOpDurationPoint{
			Timestamp:     formatTime(r.Timestamp),
			OperationName: r.OperationName,
			P50Ms:         secondsToMs(r.P50),
			P95Ms:         secondsToMs(r.P95),
			P99Ms:         secondsToMs(r.P99),
		}
	}
	return out, nil
}

// E2E latency — 1 query, 3 metrics. The query already GROUPs BY (display_bucket,
// topic, metric_name) SQL-side, so each row carries the bf16-merged percentiles
// for one of the 3 e2e metrics. Service collates the 3 metric_name rows into a
// single E2ELatencyPoint per (display_bucket, topic).
func (s *KafkaService) GetE2ELatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]E2ELatencyPoint, error) {
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
		p95 := secondsToMs(r.P95)
		switch r.MetricName {
		case MetricPublishDuration:
			t.publishP95 = p95
		case MetricReceiveDuration:
			t.receiveP95 = p95
		case MetricProcessDuration:
			t.processP95 = p95
		}
	}
	out := make([]E2ELatencyPoint, 0, len(acc))
	for k, t := range acc {
		out = append(out, E2ELatencyPoint{
			Timestamp:    formatTime(k.ts),
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

// Lag panels.

func (s *KafkaService) GetConsumerLagByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]LagPoint, error) {
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
			Timestamp:     formatTime(k.ts),
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

func (s *KafkaService) GetConsumerLagPerPartition(ctx context.Context, teamID int64, startMs, endMs int64) ([]PartitionLag, error) {
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

// Rebalance signals — 1 query, 6 metrics, fold per (bucket, group).

func (s *KafkaService) GetRebalanceSignals(ctx context.Context, teamID int64, startMs, endMs int64) ([]RebalancePoint, error) {
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
		case MetricRebalanceCount:
			x.rebalance += r.Value
		case MetricJoinCount:
			x.join += r.Value
		case MetricSyncCount:
			x.sync += r.Value
		case MetricHeartbeatCount:
			x.heartbeat += r.Value
		case MetricFailedHeartbeatCount:
			x.failed += r.Value
		case MetricAssignedPartitions:
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
			Timestamp:           formatTime(k.ts),
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

// Error panels — counter sums per (bucket, dim, error_type) divided by bucket secs.

func (s *KafkaService) GetPublishErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	rows, err := s.repo.QueryPublishErrorsByTopic(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := foldErrorRateByPair(rows,
		func(r TopicErrorCounterRow) time.Time { return r.Timestamp },
		func(r TopicErrorCounterRow) string { return r.Topic },
		func(r TopicErrorCounterRow) string { return r.ErrorType },
		func(r TopicErrorCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]ErrorRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = ErrorRatePoint{Timestamp: formatTime(fld.ts), Topic: fld.dim, ErrorType: fld.errorType, ErrorRate: fld.rate}
	}
	return out, nil
}

func (s *KafkaService) GetConsumeErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	rows, err := s.repo.QueryReceiveErrorsByGroup(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := foldErrorRateByPair(rows,
		func(r GroupErrorCounterRow) time.Time { return r.Timestamp },
		func(r GroupErrorCounterRow) string { return r.ConsumerGroup },
		func(r GroupErrorCounterRow) string { return r.ErrorType },
		func(r GroupErrorCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]ErrorRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = ErrorRatePoint{Timestamp: formatTime(fld.ts), ConsumerGroup: fld.dim, ErrorType: fld.errorType, ErrorRate: fld.rate}
	}
	return out, nil
}

func (s *KafkaService) GetProcessErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	rows, err := s.repo.QueryProcessErrorsByGroup(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := foldErrorRateByPair(rows,
		func(r GroupErrorCounterRow) time.Time { return r.Timestamp },
		func(r GroupErrorCounterRow) string { return r.ConsumerGroup },
		func(r GroupErrorCounterRow) string { return r.ErrorType },
		func(r GroupErrorCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]ErrorRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = ErrorRatePoint{Timestamp: formatTime(fld.ts), ConsumerGroup: fld.dim, ErrorType: fld.errorType, ErrorRate: fld.rate}
	}
	return out, nil
}

func (s *KafkaService) GetClientOpErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	rows, err := s.repo.QueryClientOpErrorsByOperation(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	folds := foldErrorRateByPair(rows,
		func(r OperationErrorCounterRow) time.Time { return r.Timestamp },
		func(r OperationErrorCounterRow) string { return r.OperationName },
		func(r OperationErrorCounterRow) string { return r.ErrorType },
		func(r OperationErrorCounterRow) float64 { return r.Value },
		startMs, endMs)
	out := make([]ErrorRatePoint, len(folds))
	for i, fld := range folds {
		out[i] = ErrorRatePoint{Timestamp: formatTime(fld.ts), OperationName: fld.dim, ErrorType: fld.errorType, ErrorRate: fld.rate}
	}
	return out, nil
}

// Broker connections — gauge avg per (bucket, broker).

func (s *KafkaService) GetBrokerConnections(ctx context.Context, teamID int64, startMs, endMs int64) ([]BrokerConnectionPoint, error) {
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
			Timestamp:   formatTime(k.ts),
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

// Sample queries — used by saturation/database/explorer for snapshot tables.
// Pure delegation: argMax pushes "latest by key" into SQL so the repo returns
// one row per (dim, metric) tuple.

func (s *KafkaService) GetConsumerMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]ConsumerMetricSample, error) {
	return s.repo.QueryConsumerMetricSamples(ctx, teamID, startMs, endMs, metricNames)
}

func (s *KafkaService) GetTopicMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]TopicMetricSample, error) {
	return s.repo.QueryTopicMetricSamples(ctx, teamID, startMs, endMs, metricNames)
}

// Generic folds + helpers.

type counterRateFold struct {
	ts   time.Time
	dim  string
	rate float64
}

// foldCounterRateByDim sums values per (display_bucket, dim), then divides by
// the bucket-grain seconds to produce a rate. Caller passes a typed row plus
// extractor closures so the helper works with TopicCounterRow / GroupCounterRow / etc.
func foldCounterRateByDim[R any](rows []R, tsOf func(R) time.Time, dimOf func(R) string, valOf func(R) float64, startMs, endMs int64) []counterRateFold {
	type key struct {
		ts  time.Time
		dim string
	}
	sums := map[key]float64{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(tsOf(r).Unix(), windowMs), dim: dimOf(r)}
		sums[k] += valOf(r)
	}
	bucketSecs := timebucket.DisplayGrain(windowMs).Seconds()
	if bucketSecs <= 0 {
		bucketSecs = 1
	}
	out := make([]counterRateFold, 0, len(sums))
	for k, v := range sums {
		out = append(out, counterRateFold{ts: k.ts, dim: k.dim, rate: v / bucketSecs})
	}
	slices.SortFunc(out, func(a, b counterRateFold) int {
		if c := a.ts.Compare(b.ts); c != 0 {
			return c
		}
		return cmp.Compare(a.dim, b.dim)
	})
	return out
}

type errorRateFold struct {
	ts        time.Time
	dim       string
	errorType string
	rate      float64
}

// foldErrorRateByPair sums values per (bucket, dim, error_type) and divides
// by bucket-grain seconds. Used by all four error panels.
func foldErrorRateByPair[R any](rows []R, tsOf func(R) time.Time, dimOf, errOf func(R) string, valOf func(R) float64, startMs, endMs int64) []errorRateFold {
	type key struct {
		ts        time.Time
		dim       string
		errorType string
	}
	sums := map[key]float64{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{
			ts:        timebucket.DisplayBucket(tsOf(r).Unix(), windowMs),
			dim:       dimOf(r),
			errorType: errOf(r),
		}
		sums[k] += valOf(r)
	}
	bucketSecs := timebucket.DisplayGrain(windowMs).Seconds()
	if bucketSecs <= 0 {
		bucketSecs = 1
	}
	out := make([]errorRateFold, 0, len(sums))
	for k, v := range sums {
		rate := v / bucketSecs
		if math.IsNaN(rate) {
			rate = 0
		}
		out = append(out, errorRateFold{ts: k.ts, dim: k.dim, errorType: k.errorType, rate: rate})
	}
	slices.SortFunc(out, func(a, b errorRateFold) int {
		if c := a.ts.Compare(b.ts); c != 0 {
			return c
		}
		if c := cmp.Compare(b.rate, a.rate); c != 0 {
			return c
		}
		if c := cmp.Compare(a.dim, b.dim); c != 0 {
			return c
		}
		return cmp.Compare(a.errorType, b.errorType)
	})
	return out
}

func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}
