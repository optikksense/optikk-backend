package explorer

import (
	"context"
	"sort"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka/consumer"
	"golang.org/x/sync/errgroup"
)

const (
	kafkaMetricBytesConsumedRate      = "kafka.consumer.bytes_consumed_rate"
	kafkaMetricBytesConsumedTotal     = "kafka.consumer.bytes_consumed_total"
	kafkaMetricRecordsConsumedRate    = "kafka.consumer.records_consumed_rate"
	kafkaMetricRecordsConsumedTotal   = "kafka.consumer.records_consumed_total"
	kafkaMetricRecordsLag             = "kafka.consumer.records_lag"
	kafkaMetricRecordsLead            = "kafka.consumer.records_lead"
	kafkaMetricAssignedPartitions     = "kafka.consumer.assigned_partitions"
	kafkaMetricCommitRate             = "kafka.consumer.commit_rate"
	kafkaMetricCommitLatencyAvg       = "kafka.consumer.commit_latency_avg"
	kafkaMetricCommitLatencyMax       = "kafka.consumer.commit_latency_max"
	kafkaMetricFetchRate              = "kafka.consumer.fetch_rate"
	kafkaMetricFetchLatencyAvg        = "kafka.consumer.fetch_latency_avg"
	kafkaMetricFetchLatencyMax        = "kafka.consumer.fetch_latency_max"
	kafkaMetricHeartbeatRate          = "kafka.consumer.heartbeat_rate"
	kafkaMetricFailedRebalancePerHour = "kafka.consumer.failed_rebalance_rate_per_hour"
	kafkaMetricPollIdleRatioAvg       = "kafka.consumer.poll_idle_ratio_avg"
	kafkaMetricLastPollSecondsAgo     = "kafka.consumer.last_poll_seconds_ago"
	kafkaMetricConnectionCount        = "kafka.consumer.connection_count"
)

var kafkaTopicMetricNames = []string{
	kafkaMetricBytesConsumedRate,
	kafkaMetricBytesConsumedTotal,
	kafkaMetricRecordsConsumedRate,
	kafkaMetricRecordsConsumedTotal,
	kafkaMetricRecordsLag,
	kafkaMetricRecordsLead,
}

var kafkaGroupMetricNames = []string{
	kafkaMetricAssignedPartitions,
	kafkaMetricCommitRate,
	kafkaMetricCommitLatencyAvg,
	kafkaMetricCommitLatencyMax,
	kafkaMetricFetchRate,
	kafkaMetricFetchLatencyAvg,
	kafkaMetricFetchLatencyMax,
	kafkaMetricHeartbeatRate,
	kafkaMetricFailedRebalancePerHour,
	kafkaMetricPollIdleRatioAvg,
	kafkaMetricLastPollSecondsAgo,
	kafkaMetricConnectionCount,
}

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetKafkaSummary(ctx context.Context, teamID int64, startMs, endMs int64) (KafkaSummaryResponse, error) {
	var topics []KafkaTopicRow
	var groups []KafkaGroupRow

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		topics, err = s.buildKafkaTopicRows(gctx, teamID, startMs, endMs, "", "")
		return err
	})

	g.Go(func() error {
		var err error
		groups, err = s.buildKafkaGroupRows(gctx, teamID, startMs, endMs, "", "")
		return err
	})

	if err := g.Wait(); err != nil {
		return KafkaSummaryResponse{}, err
	}

	summary := KafkaSummaryResponse{
		TopicCount: len(topics),
		GroupCount: len(groups),
	}
	for _, row := range topics {
		summary.BytesPerSec += row.BytesPerSec
	}
	for _, row := range groups {
		summary.AssignedPartitions += row.AssignedPartitions
	}
	return summary, nil
}

func (s *Service) GetKafkaTopics(ctx context.Context, teamID int64, startMs, endMs int64) ([]KafkaTopicRow, error) {
	return s.buildKafkaTopicRows(ctx, teamID, startMs, endMs, "", "")
}

func (s *Service) GetKafkaGroups(ctx context.Context, teamID int64, startMs, endMs int64) ([]KafkaGroupRow, error) {
	return s.buildKafkaGroupRows(ctx, teamID, startMs, endMs, "", "")
}

func (s *Service) GetKafkaTopicOverview(ctx context.Context, teamID int64, startMs, endMs int64, topic string) (KafkaTopicOverview, error) {
	rows, err := s.buildKafkaTopicRows(ctx, teamID, startMs, endMs, topic, "")
	if err != nil {
		return KafkaTopicOverview{}, err
	}
	overview := KafkaTopicOverview{
		Topic: topic,
		Trend: []KafkaTopicTrendPoint{},
	}
	if len(rows) > 0 {
		overview.Summary = rows[0]
	}
	return overview, nil
}

func (s *Service) GetKafkaTopicGroups(ctx context.Context, teamID int64, startMs, endMs int64, topic string) ([]KafkaTopicConsumerRow, error) {
	return s.buildKafkaTopicConsumerRows(ctx, teamID, startMs, endMs, topic, "")
}

func (s *Service) GetKafkaTopicPartitions(ctx context.Context, teamID int64, startMs, endMs int64, topic string) ([]consumer.PartitionLag, error) {
	return []consumer.PartitionLag{}, nil
}

func (s *Service) GetKafkaGroupOverview(ctx context.Context, teamID int64, startMs, endMs int64, group string) (KafkaGroupOverview, error) {
	rows, err := s.buildKafkaGroupRows(ctx, teamID, startMs, endMs, "", group)
	if err != nil {
		return KafkaGroupOverview{}, err
	}
	overview := KafkaGroupOverview{
		ConsumerGroup: group,
		Trend:         []KafkaGroupTrendPoint{},
	}
	if len(rows) > 0 {
		overview.Summary = rows[0]
	}
	return overview, nil
}

func (s *Service) GetKafkaGroupTopics(ctx context.Context, teamID int64, startMs, endMs int64, group string) ([]KafkaTopicRow, error) {
	return s.buildKafkaTopicRows(ctx, teamID, startMs, endMs, "", group)
}

func (s *Service) GetKafkaGroupPartitions(ctx context.Context, teamID int64, startMs, endMs int64, group string) ([]consumer.PartitionLag, error) {
	return []consumer.PartitionLag{}, nil
}

func (s *Service) buildKafkaTopicRows(ctx context.Context, teamID int64, startMs, endMs int64, filterTopic, filterGroup string) ([]KafkaTopicRow, error) {
	samples, err := s.repo.QueryTopicMetricSamples(ctx, teamID, startMs, endMs, kafkaTopicMetricNames)
	if err != nil {
		return nil, err
	}

	latestByKey := map[string]sampleValue{}
	consumerGroupsByTopic := map[string]map[string]struct{}{}
	for _, sample := range samples {
		topic := strings.TrimSpace(sample.Topic)
		consumerGroup := strings.TrimSpace(sample.ConsumerGroup)
		if topic == "" {
			continue
		}
		if filterTopic != "" && topic != filterTopic {
			continue
		}
		if filterGroup != "" && consumerGroup != filterGroup {
			continue
		}
		if consumerGroup != "" {
			if consumerGroupsByTopic[topic] == nil {
				consumerGroupsByTopic[topic] = map[string]struct{}{}
			}
			consumerGroupsByTopic[topic][consumerGroup] = struct{}{}
		}
		key := topic + "\x00" + consumerGroup + "\x00" + sample.MetricName
		if current, ok := latestByKey[key]; !ok || sample.Timestamp >= current.Timestamp {
			latestByKey[key] = sampleValue{Timestamp: sample.Timestamp, Value: sample.Value}
		}
	}

	rowsByTopic := map[string]*KafkaTopicRow{}
	for key, value := range latestByKey {
		topic, _, metricName := splitKafkaKey(key)
		row := ensureKafkaTopicRow(rowsByTopic, topic)
		switch metricName {
		case kafkaMetricBytesConsumedRate:
			row.BytesPerSec += value.Value
		case kafkaMetricBytesConsumedTotal:
			row.BytesTotal += value.Value
		case kafkaMetricRecordsConsumedRate:
			row.RecordsPerSec += value.Value
		case kafkaMetricRecordsConsumedTotal:
			row.RecordsTotal += value.Value
		case kafkaMetricRecordsLag:
			if value.Value > row.Lag {
				row.Lag = value.Value
			}
		case kafkaMetricRecordsLead:
			if value.Value > row.Lead {
				row.Lead = value.Value
			}
		}
	}

	rows := make([]KafkaTopicRow, 0, len(rowsByTopic))
	for topic, row := range rowsByTopic {
		if groups := consumerGroupsByTopic[topic]; groups != nil {
			row.ConsumerGroupCount = len(groups)
		}
		rows = append(rows, *row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Lag == rows[j].Lag {
			if rows[i].BytesPerSec == rows[j].BytesPerSec {
				return rows[i].Topic < rows[j].Topic
			}
			return rows[i].BytesPerSec > rows[j].BytesPerSec
		}
		return rows[i].Lag > rows[j].Lag
	})
	return rows, nil
}

func (s *Service) buildKafkaGroupRows(ctx context.Context, teamID int64, startMs, endMs int64, filterTopic, filterGroup string) ([]KafkaGroupRow, error) {
	var groupSamples []ConsumerMetricSample
	var topicSamples []TopicMetricSample

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		groupSamples, err = s.repo.QueryConsumerMetricSamples(gctx, teamID, startMs, endMs, kafkaGroupMetricNames)
		return err
	})

	g.Go(func() error {
		var err error
		topicSamples, err = s.repo.QueryTopicMetricSamples(gctx, teamID, startMs, endMs, kafkaTopicMetricNames)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	latestByKey := map[string]sampleValue{}
	for _, sample := range groupSamples {
		consumerGroup := strings.TrimSpace(sample.ConsumerGroup)
		if consumerGroup == "" {
			continue
		}
		if filterGroup != "" && consumerGroup != filterGroup {
			continue
		}
		key := consumerGroup + "\x00" + strings.TrimSpace(sample.NodeID) + "\x00" + sample.MetricName
		if current, ok := latestByKey[key]; !ok || sample.Timestamp >= current.Timestamp {
			latestByKey[key] = sampleValue{Timestamp: sample.Timestamp, Value: sample.Value}
		}
	}

	valuesByGroup := map[string]map[string][]float64{}
	for key, value := range latestByKey {
		consumerGroup, _, metricName := splitKafkaKey(key)
		if valuesByGroup[consumerGroup] == nil {
			valuesByGroup[consumerGroup] = map[string][]float64{}
		}
		valuesByGroup[consumerGroup][metricName] = append(valuesByGroup[consumerGroup][metricName], value.Value)
	}

	topicsByGroup := map[string]map[string]struct{}{}
	for _, sample := range topicSamples {
		consumerGroup := strings.TrimSpace(sample.ConsumerGroup)
		topic := strings.TrimSpace(sample.Topic)
		if consumerGroup == "" || topic == "" {
			continue
		}
		if filterTopic != "" && topic != filterTopic {
			continue
		}
		if filterGroup != "" && consumerGroup != filterGroup {
			continue
		}
		if topicsByGroup[consumerGroup] == nil {
			topicsByGroup[consumerGroup] = map[string]struct{}{}
		}
		topicsByGroup[consumerGroup][topic] = struct{}{}
	}

	groupSet := map[string]struct{}{}
	for group := range valuesByGroup {
		groupSet[group] = struct{}{}
	}
	for group := range topicsByGroup {
		groupSet[group] = struct{}{}
	}

	rows := make([]KafkaGroupRow, 0, len(groupSet))
	for _, consumerGroup := range sortedKeys(groupSet) {
		values := valuesByGroup[consumerGroup]
		row := KafkaGroupRow{
			ConsumerGroup:          consumerGroup,
			AssignedPartitions:     sumFloat(values[kafkaMetricAssignedPartitions]),
			CommitRate:             sumFloat(values[kafkaMetricCommitRate]),
			CommitLatencyAvgMs:     avgFloat(values[kafkaMetricCommitLatencyAvg]),
			CommitLatencyMaxMs:     maxFloat(values[kafkaMetricCommitLatencyMax]),
			FetchRate:              sumFloat(values[kafkaMetricFetchRate]),
			FetchLatencyAvgMs:      avgFloat(values[kafkaMetricFetchLatencyAvg]),
			FetchLatencyMaxMs:      maxFloat(values[kafkaMetricFetchLatencyMax]),
			HeartbeatRate:          sumFloat(values[kafkaMetricHeartbeatRate]),
			FailedRebalancePerHour: sumFloat(values[kafkaMetricFailedRebalancePerHour]),
			PollIdleRatio:          avgFloat(values[kafkaMetricPollIdleRatioAvg]),
			LastPollSecondsAgo:     maxFloat(values[kafkaMetricLastPollSecondsAgo]),
			ConnectionCount:        sumFloat(values[kafkaMetricConnectionCount]),
			TopicCount:             len(topicsByGroup[consumerGroup]),
		}
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].AssignedPartitions == rows[j].AssignedPartitions {
			return rows[i].ConsumerGroup < rows[j].ConsumerGroup
		}
		return rows[i].AssignedPartitions > rows[j].AssignedPartitions
	})
	return rows, nil
}

func (s *Service) buildKafkaTopicConsumerRows(ctx context.Context, teamID int64, startMs, endMs int64, filterTopic, filterGroup string) ([]KafkaTopicConsumerRow, error) {
	samples, err := s.repo.QueryTopicMetricSamples(ctx, teamID, startMs, endMs, kafkaTopicMetricNames)
	if err != nil {
		return nil, err
	}

	latestByKey := map[string]sampleValue{}
	for _, sample := range samples {
		consumerGroup := strings.TrimSpace(sample.ConsumerGroup)
		topic := strings.TrimSpace(sample.Topic)
		if consumerGroup == "" {
			continue
		}
		if filterTopic != "" && topic != filterTopic {
			continue
		}
		if filterGroup != "" && consumerGroup != filterGroup {
			continue
		}
		key := consumerGroup + "\x00" + topic + "\x00" + sample.MetricName
		if current, ok := latestByKey[key]; !ok || sample.Timestamp >= current.Timestamp {
			latestByKey[key] = sampleValue{Timestamp: sample.Timestamp, Value: sample.Value}
		}
	}

	rowsByGroup := map[string]*KafkaTopicConsumerRow{}
	for key, value := range latestByKey {
		consumerGroup, _, metricName := splitKafkaKey(key)
		row := rowsByGroup[consumerGroup]
		if row == nil {
			row = &KafkaTopicConsumerRow{ConsumerGroup: consumerGroup}
			rowsByGroup[consumerGroup] = row
		}
		switch metricName {
		case kafkaMetricBytesConsumedRate:
			row.BytesPerSec += value.Value
		case kafkaMetricRecordsConsumedRate:
			row.RecordsPerSec += value.Value
		case kafkaMetricRecordsLag:
			if value.Value > row.Lag {
				row.Lag = value.Value
			}
		case kafkaMetricRecordsLead:
			if value.Value > row.Lead {
				row.Lead = value.Value
			}
		}
	}

	rows := make([]KafkaTopicConsumerRow, 0, len(rowsByGroup))
	for _, row := range rowsByGroup {
		rows = append(rows, *row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Lag == rows[j].Lag {
			return rows[i].ConsumerGroup < rows[j].ConsumerGroup
		}
		return rows[i].Lag > rows[j].Lag
	})
	return rows, nil
}

type sampleValue struct {
	Timestamp string
	Value     float64
}

func ensureKafkaTopicRow(rows map[string]*KafkaTopicRow, topic string) *KafkaTopicRow {
	row := rows[topic]
	if row == nil {
		row = &KafkaTopicRow{Topic: topic}
		rows[topic] = row
	}
	return row
}

func splitKafkaKey(key string) (string, string, string) {
	parts := strings.SplitN(key, "\x00", 3)
	for len(parts) < 3 {
		parts = append(parts, "")
	}
	return parts[0], parts[1], parts[2]
}

func sumFloat(values []float64) float64 {
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total
}

func avgFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return sumFloat(values) / float64(len(values))
}

func maxFloat(values []float64) float64 {
	max := 0.0
	for i, value := range values {
		if i == 0 || value > max {
			max = value
		}
	}
	return max
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
