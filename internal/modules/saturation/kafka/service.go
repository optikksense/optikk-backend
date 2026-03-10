package kafka

// KafkaService provides business logic orchestration for kafka saturation metrics.
type KafkaService struct {
	repo *ClickHouseRepository
}

// NewService creates a new KafkaService.
func NewService(repo *ClickHouseRepository) *KafkaService {
	return &KafkaService{repo: repo}
}

func (s *KafkaService) GetKafkaQueueLag(teamID int64, startMs, endMs int64) ([]KafkaQueueLag, error) {
	return s.repo.GetKafkaQueueLag(teamID, startMs, endMs)
}

func (s *KafkaService) GetKafkaProductionRate(teamID int64, startMs, endMs int64) ([]KafkaProductionRate, error) {
	return s.repo.GetKafkaProductionRate(teamID, startMs, endMs)
}

func (s *KafkaService) GetKafkaConsumptionRate(teamID int64, startMs, endMs int64) ([]KafkaConsumptionRate, error) {
	return s.repo.GetKafkaConsumptionRate(teamID, startMs, endMs)
}

func (s *KafkaService) GetQueueConsumerLag(teamID int64, startMs, endMs int64) ([]MqBucket, error) {
	return s.repo.GetQueueConsumerLag(teamID, startMs, endMs)
}

func (s *KafkaService) GetQueueTopicLag(teamID int64, startMs, endMs int64) ([]MqBucket, error) {
	return s.repo.GetQueueTopicLag(teamID, startMs, endMs)
}

func (s *KafkaService) GetQueueTopQueues(teamID int64, startMs, endMs int64) ([]MqTopQueue, error) {
	return s.repo.GetQueueTopQueues(teamID, startMs, endMs)
}

func (s *KafkaService) GetConsumerLagPerPartition(teamID int64, startMs, endMs int64) ([]PartitionLag, error) {
	return s.repo.GetConsumerLagPerPartition(teamID, startMs, endMs)
}

func (s *KafkaService) GetMessageRates(teamID int64, startMs, endMs int64) (MessageRates, error) {
	return s.repo.GetMessageRates(teamID, startMs, endMs)
}

func (s *KafkaService) GetOperationDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetOperationDuration(teamID, startMs, endMs)
}

func (s *KafkaService) GetOffsetCommitRate(teamID int64, startMs, endMs int64) ([]OffsetTimeSeries, error) {
	return s.repo.GetOffsetCommitRate(teamID, startMs, endMs)
}
