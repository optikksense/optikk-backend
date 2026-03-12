package kafka

type KafkaService struct {
	repo *ClickHouseRepository
}

func NewService(repo *ClickHouseRepository) *KafkaService {
	return &KafkaService{repo: repo}
}

func (s *KafkaService) GetKafkaSummaryStats(teamID int64, startMs, endMs int64) (KafkaSummaryStats, error) {
	return s.repo.GetKafkaSummaryStats(teamID, startMs, endMs)
}

func (s *KafkaService) GetProduceRateByTopic(teamID int64, startMs, endMs int64) ([]TopicRatePoint, error) {
	return s.repo.GetProduceRateByTopic(teamID, startMs, endMs)
}

func (s *KafkaService) GetPublishLatencyByTopic(teamID int64, startMs, endMs int64) ([]TopicLatencyPoint, error) {
	return s.repo.GetPublishLatencyByTopic(teamID, startMs, endMs)
}

func (s *KafkaService) GetConsumeRateByTopic(teamID int64, startMs, endMs int64) ([]TopicRatePoint, error) {
	return s.repo.GetConsumeRateByTopic(teamID, startMs, endMs)
}

func (s *KafkaService) GetReceiveLatencyByTopic(teamID int64, startMs, endMs int64) ([]TopicLatencyPoint, error) {
	return s.repo.GetReceiveLatencyByTopic(teamID, startMs, endMs)
}

func (s *KafkaService) GetConsumeRateByGroup(teamID int64, startMs, endMs int64) ([]GroupRatePoint, error) {
	return s.repo.GetConsumeRateByGroup(teamID, startMs, endMs)
}

func (s *KafkaService) GetProcessRateByGroup(teamID int64, startMs, endMs int64) ([]GroupRatePoint, error) {
	return s.repo.GetProcessRateByGroup(teamID, startMs, endMs)
}

func (s *KafkaService) GetProcessLatencyByGroup(teamID int64, startMs, endMs int64) ([]GroupLatencyPoint, error) {
	return s.repo.GetProcessLatencyByGroup(teamID, startMs, endMs)
}

func (s *KafkaService) GetConsumerLagByGroup(teamID int64, startMs, endMs int64) ([]LagPoint, error) {
	return s.repo.GetConsumerLagByGroup(teamID, startMs, endMs)
}

func (s *KafkaService) GetConsumerLagPerPartition(teamID int64, startMs, endMs int64) ([]PartitionLag, error) {
	return s.repo.GetConsumerLagPerPartition(teamID, startMs, endMs)
}

func (s *KafkaService) GetRebalanceSignals(teamID int64, startMs, endMs int64) ([]RebalancePoint, error) {
	return s.repo.GetRebalanceSignals(teamID, startMs, endMs)
}

func (s *KafkaService) GetE2ELatency(teamID int64, startMs, endMs int64) ([]E2ELatencyPoint, error) {
	return s.repo.GetE2ELatency(teamID, startMs, endMs)
}

func (s *KafkaService) GetPublishErrors(teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	return s.repo.GetPublishErrors(teamID, startMs, endMs)
}

func (s *KafkaService) GetConsumeErrors(teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	return s.repo.GetConsumeErrors(teamID, startMs, endMs)
}

func (s *KafkaService) GetProcessErrors(teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	return s.repo.GetProcessErrors(teamID, startMs, endMs)
}

func (s *KafkaService) GetClientOpErrors(teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	return s.repo.GetClientOpErrors(teamID, startMs, endMs)
}

func (s *KafkaService) GetBrokerConnections(teamID int64, startMs, endMs int64) ([]BrokerConnectionPoint, error) {
	return s.repo.GetBrokerConnections(teamID, startMs, endMs)
}

func (s *KafkaService) GetClientOperationDuration(teamID int64, startMs, endMs int64) ([]ClientOpDurationPoint, error) {
	return s.repo.GetClientOperationDuration(teamID, startMs, endMs)
}
