package kafka

// KafkaService provides business logic orchestration for kafka saturation metrics.
type KafkaService struct {
	repo *ClickHouseRepository
}

// NewService creates a new KafkaService.
func NewService(repo *ClickHouseRepository) *KafkaService {
	return &KafkaService{repo: repo}
}

func (s *KafkaService) GetKafkaQueueLag(teamUUID string, startMs, endMs int64) ([]KafkaQueueLag, error) {
	return s.repo.GetKafkaQueueLag(teamUUID, startMs, endMs)
}

func (s *KafkaService) GetKafkaProductionRate(teamUUID string, startMs, endMs int64) ([]KafkaProductionRate, error) {
	return s.repo.GetKafkaProductionRate(teamUUID, startMs, endMs)
}

func (s *KafkaService) GetKafkaConsumptionRate(teamUUID string, startMs, endMs int64) ([]KafkaConsumptionRate, error) {
	return s.repo.GetKafkaConsumptionRate(teamUUID, startMs, endMs)
}

func (s *KafkaService) GetQueueConsumerLag(teamUUID string, startMs, endMs int64) ([]MqBucket, error) {
	return s.repo.GetQueueConsumerLag(teamUUID, startMs, endMs)
}

func (s *KafkaService) GetQueueTopicLag(teamUUID string, startMs, endMs int64) ([]MqBucket, error) {
	return s.repo.GetQueueTopicLag(teamUUID, startMs, endMs)
}

func (s *KafkaService) GetQueueTopQueues(teamUUID string, startMs, endMs int64) ([]MqTopQueue, error) {
	return s.repo.GetQueueTopQueues(teamUUID, startMs, endMs)
}

func (s *KafkaService) GetConsumerLagPerPartition(teamUUID string, startMs, endMs int64) ([]PartitionLag, error) {
	return s.repo.GetConsumerLagPerPartition(teamUUID, startMs, endMs)
}

func (s *KafkaService) GetMessageRates(teamUUID string, startMs, endMs int64) (MessageRates, error) {
	return s.repo.GetMessageRates(teamUUID, startMs, endMs)
}

func (s *KafkaService) GetOperationDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetOperationDuration(teamUUID, startMs, endMs)
}

func (s *KafkaService) GetOffsetCommitRate(teamUUID string, startMs, endMs int64) ([]OffsetTimeSeries, error) {
	return s.repo.GetOffsetCommitRate(teamUUID, startMs, endMs)
}
