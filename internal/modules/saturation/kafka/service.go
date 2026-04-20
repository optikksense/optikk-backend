package kafka

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
)

// KafkaService attaches sketch-driven percentiles and converts raw CH-native
// rows into the public DTOs. All rate / average / pivot math lives here so
// SQL in repository.go stays aggregate-free beyond plain sum + count + max.
type KafkaService struct {
	repo    Repository
	sketchQ *sketch.Querier
}

func NewService(repo Repository, sketchQ *sketch.Querier) *KafkaService {
	return &KafkaService{repo: repo, sketchQ: sketchQ}
}

// teamIDString converts an int64 tenant id to the string form used by all
// sketch keys.
func teamIDString(teamID int64) string { return fmt.Sprintf("%d", teamID) }

// formatBucket stamps time_bucket slots into the stable RFC3339 string format
// the frontend already consumes. Previously ClickHouse emitted String via
// toString(); we now scan time.Time and format in Go.
func formatBucket(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// safeDiv returns a/b, or 0 when b is zero or either side is non-finite.
func safeDiv(a, b float64) float64 {
	if b == 0 || !isFinite(a) || !isFinite(b) {
		return 0
	}
	return a / b
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

// --- Summary -----------------------------------------------------------------

// summaryP95FromSketch returns a single publish/receive p95 number merged
// across every KafkaTopicLatency dim for the tenant. The sketch does not
// separate publish from receive today, so publish_p95 and receive_p95 carry
// the same value — better than the old quantileTDigestWeightedIf path which
// ran two full-table aggregates in one SQL pass.
func (s *KafkaService) summaryP95FromSketch(ctx context.Context, teamID int64, startMs, endMs int64) float64 {
	if s.sketchQ == nil {
		return 0
	}
	pcts, err := s.sketchQ.PercentilesByDimPrefix(ctx, sketch.KafkaTopicLatency, teamIDString(teamID), startMs, endMs, []string{""}, 0.95)
	if err != nil {
		return 0
	}
	if v, ok := pcts[""]; ok && len(v) == 1 {
		return v[0] * 1000.0
	}
	return 0
}

func (s *KafkaService) GetKafkaSummaryStats(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) (KafkaSummaryStats, error) {
	windowSecs := float64(endMs-startMs) / 1000.0
	if windowSecs <= 0 {
		windowSecs = 1.0
	}

	rates, err := s.repo.GetSummaryRates(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return KafkaSummaryStats{}, err
	}
	maxLag, err := s.repo.GetSummaryMaxLag(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return KafkaSummaryStats{}, err
	}

	var result KafkaSummaryStats
	for _, r := range rates {
		switch r.MetricFamily {
		case "producer":
			result.PublishRatePerSec = safeDiv(r.ValueSum, windowSecs)
		case "consumer":
			result.ReceiveRatePerSec = safeDiv(r.ValueSum, windowSecs)
		}
	}
	result.MaxLag = maxLag.MaxLag

	p95 := s.summaryP95FromSketch(ctx, teamID, startMs, endMs)
	result.PublishP95Ms = p95
	result.ReceiveP95Ms = p95
	return result, nil
}

// --- Rate helpers ------------------------------------------------------------

func (s *KafkaService) GetProduceRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicRatePoint, error) {
	rows, err := s.repo.GetProduceRateByTopic(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	return topicRatePoints(rows, bucketSecs(startMs, endMs)), nil
}

func (s *KafkaService) GetConsumeRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicRatePoint, error) {
	rows, err := s.repo.GetConsumeRateByTopic(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	return topicRatePoints(rows, bucketSecs(startMs, endMs)), nil
}

func topicRatePoints(rows []topicValueSumRow, bs float64) []TopicRatePoint {
	out := make([]TopicRatePoint, len(rows))
	for i, r := range rows {
		out[i] = TopicRatePoint{
			Timestamp:  formatBucket(r.TimeBucket),
			Topic:      r.Topic,
			RatePerSec: safeDiv(r.ValueSum, bs),
		}
	}
	return out
}

func (s *KafkaService) GetConsumeRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupRatePoint, error) {
	rows, err := s.repo.GetConsumeRateByGroup(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	return groupRatePoints(rows, bucketSecs(startMs, endMs)), nil
}

func (s *KafkaService) GetProcessRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupRatePoint, error) {
	rows, err := s.repo.GetProcessRateByGroup(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	return groupRatePoints(rows, bucketSecs(startMs, endMs)), nil
}

func groupRatePoints(rows []groupValueSumRow, bs float64) []GroupRatePoint {
	out := make([]GroupRatePoint, len(rows))
	for i, r := range rows {
		out[i] = GroupRatePoint{
			Timestamp:     formatBucket(r.TimeBucket),
			ConsumerGroup: r.ConsumerGroup,
			RatePerSec:    safeDiv(r.ValueSum, bs),
		}
	}
	return out
}

// --- Latency helpers (sketch-backed) -----------------------------------------

// topicPercentilesFromSketch fetches (p50, p95, p99) merged per topic from the
// KafkaTopicLatency sketch (dim tuple = topic|client_id). segmentIdx=0 selects
// the topic segment; values come from the unique topic set in rows.
func (s *KafkaService) topicPercentilesFromSketch(ctx context.Context, teamID int64, startMs, endMs int64, topics []string) map[string][]float64 {
	if s.sketchQ == nil || len(topics) == 0 {
		return nil
	}
	pcts, err := s.sketchQ.PercentilesByDimSegment(ctx, sketch.KafkaTopicLatency, teamIDString(teamID), startMs, endMs, 0, topics, 0.50, 0.95, 0.99)
	if err != nil {
		return nil
	}
	return pcts
}

func (s *KafkaService) groupPercentilesFromSketch(ctx context.Context, teamID int64, startMs, endMs int64, groups []string) map[string][]float64 {
	if s.sketchQ == nil || len(groups) == 0 {
		return nil
	}
	pcts, err := s.sketchQ.PercentilesByDimSegment(ctx, sketch.KafkaTopicLatency, teamIDString(teamID), startMs, endMs, 1, groups, 0.50, 0.95, 0.99)
	if err != nil {
		return nil
	}
	return pcts
}

func (s *KafkaService) GetPublishLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicLatencyPoint, error) {
	return s.topicLatencyPoints(ctx, teamID, startMs, endMs, f, s.repo.GetPublishLatencyKeys)
}

func (s *KafkaService) GetReceiveLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicLatencyPoint, error) {
	return s.topicLatencyPoints(ctx, teamID, startMs, endMs, f, s.repo.GetReceiveLatencyKeys)
}

func (s *KafkaService) topicLatencyPoints(
	ctx context.Context,
	teamID int64, startMs, endMs int64, f KafkaFilters,
	fetch func(context.Context, int64, int64, int64, KafkaFilters) ([]topicKeyRow, error),
) ([]TopicLatencyPoint, error) {
	rows, err := fetch(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	topics := uniqueTopics(rows)
	pcts := s.topicPercentilesFromSketch(ctx, teamID, startMs, endMs, topics)

	out := make([]TopicLatencyPoint, len(rows))
	for i, r := range rows {
		p := TopicLatencyPoint{
			Timestamp: formatBucket(r.TimeBucket),
			Topic:     r.Topic,
		}
		if v, ok := pcts[r.Topic]; ok && len(v) == 3 {
			p.P50Ms = v[0] * 1000.0
			p.P95Ms = v[1] * 1000.0
			p.P99Ms = v[2] * 1000.0
		}
		out[i] = p
	}
	return out, nil
}

func (s *KafkaService) GetProcessLatencyByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupLatencyPoint, error) {
	rows, err := s.repo.GetProcessLatencyKeys(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	groups := uniqueGroups(rows)
	pcts := s.groupPercentilesFromSketch(ctx, teamID, startMs, endMs, groups)

	out := make([]GroupLatencyPoint, len(rows))
	for i, r := range rows {
		p := GroupLatencyPoint{
			Timestamp:     formatBucket(r.TimeBucket),
			ConsumerGroup: r.ConsumerGroup,
		}
		if v, ok := pcts[r.ConsumerGroup]; ok && len(v) == 3 {
			p.P50Ms = v[0] * 1000.0
			p.P95Ms = v[1] * 1000.0
			p.P99Ms = v[2] * 1000.0
		}
		out[i] = p
	}
	return out, nil
}

// --- Consumer lag ------------------------------------------------------------

func (s *KafkaService) GetConsumerLagByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]LagPoint, error) {
	rows, err := s.repo.GetConsumerLagByGroup(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	out := make([]LagPoint, len(rows))
	for i, r := range rows {
		out[i] = LagPoint{
			Timestamp:     formatBucket(r.TimeBucket),
			ConsumerGroup: r.ConsumerGroup,
			Topic:         r.Topic,
			Lag:           safeDiv(r.ValueSum, float64(r.ValueCount)),
		}
	}
	return out, nil
}

func (s *KafkaService) GetConsumerLagPerPartition(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]PartitionLag, error) {
	rows, err := s.repo.GetConsumerLagPerPartition(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	out := make([]PartitionLag, 0, len(rows))
	for _, r := range rows {
		part, _ := strconv.ParseInt(r.Partition, 10, 64) // empty / non-numeric → 0
		avg := safeDiv(r.ValueSum, float64(r.ValueCount))
		lag := int64(0)
		if isFinite(avg) {
			lag = int64(math.Round(avg))
		}
		out = append(out, PartitionLag{
			Topic:         r.Topic,
			Partition:     part,
			ConsumerGroup: r.ConsumerGroup,
			Lag:           lag,
		})
	}
	return out, nil
}

// --- Rebalance signals -------------------------------------------------------

// GetRebalanceSignals pivots the per-metric_name rows into the RebalancePoint
// shape, converting counters to per-second rates and the assigned-partitions
// gauge to its bucket-average via sum/count.
func (s *KafkaService) GetRebalanceSignals(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]RebalancePoint, error) {
	rows, err := s.repo.GetRebalanceMetrics(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}

	bs := bucketSecs(startMs, endMs)

	type key struct {
		ts    time.Time
		group string
	}
	pivot := map[key]*RebalancePoint{}
	order := []key{}

	get := func(k key) *RebalancePoint {
		if p, ok := pivot[k]; ok {
			return p
		}
		p := &RebalancePoint{
			Timestamp:     formatBucket(k.ts),
			ConsumerGroup: k.group,
		}
		pivot[k] = p
		order = append(order, k)
		return p
	}

	for _, r := range rows {
		k := key{ts: r.TimeBucket, group: r.ConsumerGroup}
		p := get(k)
		switch r.MetricName {
		case MetricRebalanceCount:
			p.RebalanceRate = safeDiv(r.ValueSum, bs)
		case MetricJoinCount:
			p.JoinRate = safeDiv(r.ValueSum, bs)
		case MetricSyncCount:
			p.SyncRate = safeDiv(r.ValueSum, bs)
		case MetricHeartbeatCount:
			p.HeartbeatRate = safeDiv(r.ValueSum, bs)
		case MetricFailedHeartbeatCount:
			p.FailedHeartbeatRate = safeDiv(r.ValueSum, bs)
		case MetricAssignedPartitions:
			p.AssignedPartitions = safeDiv(r.ValueSum, float64(r.ValueCount))
		}
	}

	out := make([]RebalancePoint, len(order))
	for i, k := range order {
		out[i] = *pivot[k]
	}
	return out, nil
}

// --- End-to-end latency ------------------------------------------------------

func (s *KafkaService) GetE2ELatency(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]E2ELatencyPoint, error) {
	rows, err := s.repo.GetE2ELatencyKeys(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	topics := uniqueTopics(rows)
	pcts := s.topicPercentilesFromSketch(ctx, teamID, startMs, endMs, topics)

	out := make([]E2ELatencyPoint, len(rows))
	for i, r := range rows {
		p := E2ELatencyPoint{
			Timestamp: formatBucket(r.TimeBucket),
			Topic:     r.Topic,
		}
		// KafkaTopicLatency is a single sketch per (topic, client-id) that
		// aggregates publish / consume / process together — all three fields
		// surface the same p95 until a per-operation sketch kind lands.
		if v, ok := pcts[r.Topic]; ok && len(v) == 3 {
			p95 := v[1] * 1000.0
			p.PublishP95Ms = p95
			p.ReceiveP95Ms = p95
			p.ProcessP95Ms = p95
		}
		out[i] = p
	}
	return out, nil
}

// --- Errors ------------------------------------------------------------------

func (s *KafkaService) GetPublishErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return s.errorRatePoints(ctx, teamID, startMs, endMs, f, s.repo.GetPublishErrors)
}

func (s *KafkaService) GetConsumeErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return s.errorRatePoints(ctx, teamID, startMs, endMs, f, s.repo.GetConsumeErrors)
}

func (s *KafkaService) GetProcessErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return s.errorRatePoints(ctx, teamID, startMs, endMs, f, s.repo.GetProcessErrors)
}

func (s *KafkaService) GetClientOpErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return s.errorRatePoints(ctx, teamID, startMs, endMs, f, s.repo.GetClientOpErrors)
}

func (s *KafkaService) errorRatePoints(
	ctx context.Context,
	teamID int64, startMs, endMs int64, f KafkaFilters,
	fetch func(context.Context, int64, int64, int64, KafkaFilters) ([]errorRateRawRow, error),
) ([]ErrorRatePoint, error) {
	rows, err := fetch(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	bs := bucketSecs(startMs, endMs)

	out := make([]ErrorRatePoint, len(rows))
	for i, r := range rows {
		out[i] = ErrorRatePoint{
			Timestamp:     formatBucket(r.TimeBucket),
			Topic:         r.Topic,
			ConsumerGroup: r.ConsumerGroup,
			OperationName: r.OperationName,
			ErrorType:     r.ErrorType,
			ErrorRate:     safeDiv(r.ValueSum, bs),
		}
	}
	return out, nil
}

// --- Broker connections ------------------------------------------------------

func (s *KafkaService) GetBrokerConnections(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]BrokerConnectionPoint, error) {
	rows, err := s.repo.GetBrokerConnections(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	out := make([]BrokerConnectionPoint, len(rows))
	for i, r := range rows {
		out[i] = BrokerConnectionPoint{
			Timestamp:   formatBucket(r.TimeBucket),
			Broker:      r.Broker,
			Connections: safeDiv(r.ValueSum, float64(r.ValueCount)),
		}
	}
	return out, nil
}

// GetClientOperationDuration keys off unique operation names but leaves
// percentile fields at zero — the current sketch topology has no matching
// dimension for messaging.client.operation.duration grouped by operation.
// Tracked as a follow-up (new sketch kind keyed by operation_name).
func (s *KafkaService) GetClientOperationDuration(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ClientOpDurationPoint, error) {
	rows, err := s.repo.GetClientOperationDurationKeys(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	out := make([]ClientOpDurationPoint, len(rows))
	for i, r := range rows {
		out[i] = ClientOpDurationPoint{
			Timestamp:     formatBucket(r.TimeBucket),
			OperationName: r.OperationName,
		}
	}
	return out, nil
}

// --- Inventory samples -------------------------------------------------------

func (s *KafkaService) GetConsumerMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricNames []string) ([]ConsumerMetricSample, error) {
	rows, err := s.repo.GetConsumerMetricSamples(ctx, teamID, startMs, endMs, f, metricNames)
	if err != nil {
		return nil, err
	}
	out := make([]ConsumerMetricSample, len(rows))
	for i, r := range rows {
		out[i] = ConsumerMetricSample{
			Timestamp:     formatBucket(r.TimeBucket),
			ConsumerGroup: r.ConsumerGroup,
			NodeID:        r.NodeID,
			MetricName:    r.MetricName,
			Value:         safeDiv(r.ValueSum, float64(r.ValueCount)),
		}
	}
	return out, nil
}

func (s *KafkaService) GetTopicMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricNames []string) ([]TopicMetricSample, error) {
	rows, err := s.repo.GetTopicMetricSamples(ctx, teamID, startMs, endMs, f, metricNames)
	if err != nil {
		return nil, err
	}
	out := make([]TopicMetricSample, len(rows))
	for i, r := range rows {
		out[i] = TopicMetricSample{
			Timestamp:     formatBucket(r.TimeBucket),
			Topic:         r.Topic,
			ConsumerGroup: r.ConsumerGroup,
			MetricName:    r.MetricName,
			Value:         safeDiv(r.ValueSum, float64(r.ValueCount)),
		}
	}
	return out, nil
}

// --- helpers -----------------------------------------------------------------

func uniqueTopics(rows []topicKeyRow) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if _, ok := seen[r.Topic]; ok {
			continue
		}
		seen[r.Topic] = struct{}{}
		out = append(out, r.Topic)
	}
	return out
}

func uniqueGroups(rows []groupKeyRow) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if _, ok := seen[r.ConsumerGroup]; ok {
			continue
		}
		seen[r.ConsumerGroup] = struct{}{}
		out = append(out, r.ConsumerGroup)
	}
	return out
}
