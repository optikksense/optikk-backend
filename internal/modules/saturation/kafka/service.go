package kafka

import "context"

type KafkaService struct {
	repo *ClickHouseRepository
}

func NewService(repo *ClickHouseRepository) *KafkaService {
	return &KafkaService{repo: repo}
}

func (s *KafkaService) GetKafkaSummaryStats(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) (KafkaSummaryStats, error) {
	return s.repo.GetKafkaSummaryStats(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetProduceRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicRatePoint, error) {
	return s.repo.GetProduceRateByTopic(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetPublishLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicLatencyPoint, error) {
	return s.repo.GetPublishLatencyByTopic(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetConsumeRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicRatePoint, error) {
	return s.repo.GetConsumeRateByTopic(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetReceiveLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicLatencyPoint, error) {
	return s.repo.GetReceiveLatencyByTopic(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetConsumeRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupRatePoint, error) {
	return s.repo.GetConsumeRateByGroup(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetProcessRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupRatePoint, error) {
	return s.repo.GetProcessRateByGroup(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetProcessLatencyByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupLatencyPoint, error) {
	return s.repo.GetProcessLatencyByGroup(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetConsumerLagByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]LagPoint, error) {
	return s.repo.GetConsumerLagByGroup(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetConsumerLagPerPartition(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]PartitionLag, error) {
	return s.repo.GetConsumerLagPerPartition(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetRebalanceSignals(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]RebalancePoint, error) {
	return s.repo.GetRebalanceSignals(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetE2ELatency(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]E2ELatencyPoint, error) {
	return s.repo.GetE2ELatency(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetPublishErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return s.repo.GetPublishErrors(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetConsumeErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return s.repo.GetConsumeErrors(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetProcessErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return s.repo.GetProcessErrors(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetClientOpErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return s.repo.GetClientOpErrors(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetBrokerConnections(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]BrokerConnectionPoint, error) {
	return s.repo.GetBrokerConnections(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetClientOperationDuration(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ClientOpDurationPoint, error) {
	return s.repo.GetClientOperationDuration(ctx, teamID, startMs, endMs, f)
}

func (s *KafkaService) GetConsumerMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricNames []string) ([]ConsumerMetricSample, error) {
	return s.repo.GetConsumerMetricSamples(ctx, teamID, startMs, endMs, f, metricNames)
}

func (s *KafkaService) GetTopicMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricNames []string) ([]TopicMetricSample, error) {
	return s.repo.GetTopicMetricSamples(ctx, teamID, startMs, endMs, f, metricNames)
}
