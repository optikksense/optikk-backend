package ai

// Service encapsulates the business logic for AI models.
type Service interface {
	GetAISummary(teamUUID string, startMs, endMs int64) (*AISummary, error)
	GetAIModels(teamUUID string, startMs, endMs int64) ([]AIModel, error)
	GetAIPerformanceMetrics(teamUUID string, startMs, endMs int64) ([]AIPerformanceMetric, error)
	GetAIPerformanceTimeSeries(teamUUID string, startMs, endMs int64) ([]AIPerformanceTimeSeries, error)
	GetAILatencyHistogram(teamUUID string, modelName string, startMs, endMs int64) ([]AILatencyHistogram, error)
	GetAICostMetrics(teamUUID string, startMs, endMs int64) ([]AICostMetric, error)
	GetAICostTimeSeries(teamUUID string, startMs, endMs int64) ([]AICostTimeSeries, error)
	GetAITokenBreakdown(teamUUID string, startMs, endMs int64) ([]AITokenBreakdown, error)
	GetAISecurityMetrics(teamUUID string, startMs, endMs int64) ([]AISecurityMetric, error)
	GetAISecurityTimeSeries(teamUUID string, startMs, endMs int64) ([]AISecurityTimeSeries, error)
	GetAIPiiCategories(teamUUID string, startMs, endMs int64) ([]AIPiiCategory, error)
}

// AIService provides business logic orchestration for AI models.
type AIService struct {
	repo Repository
}

// NewService creates a new AIService.
func NewService(repo Repository) *AIService {
	return &AIService{repo: repo}
}

func (s *AIService) GetAISummary(teamUUID string, startMs, endMs int64) (*AISummary, error) {
	return s.repo.GetAISummary(teamUUID, startMs, endMs)
}

func (s *AIService) GetAIModels(teamUUID string, startMs, endMs int64) ([]AIModel, error) {
	return s.repo.GetAIModels(teamUUID, startMs, endMs)
}

func (s *AIService) GetAIPerformanceMetrics(teamUUID string, startMs, endMs int64) ([]AIPerformanceMetric, error) {
	return s.repo.GetAIPerformanceMetrics(teamUUID, startMs, endMs)
}

func (s *AIService) GetAIPerformanceTimeSeries(teamUUID string, startMs, endMs int64) ([]AIPerformanceTimeSeries, error) {
	return s.repo.GetAIPerformanceTimeSeries(teamUUID, startMs, endMs)
}

func (s *AIService) GetAILatencyHistogram(teamUUID string, modelName string, startMs, endMs int64) ([]AILatencyHistogram, error) {
	return s.repo.GetAILatencyHistogram(teamUUID, modelName, startMs, endMs)
}

func (s *AIService) GetAICostMetrics(teamUUID string, startMs, endMs int64) ([]AICostMetric, error) {
	return s.repo.GetAICostMetrics(teamUUID, startMs, endMs)
}

func (s *AIService) GetAICostTimeSeries(teamUUID string, startMs, endMs int64) ([]AICostTimeSeries, error) {
	return s.repo.GetAICostTimeSeries(teamUUID, startMs, endMs)
}

func (s *AIService) GetAITokenBreakdown(teamUUID string, startMs, endMs int64) ([]AITokenBreakdown, error) {
	return s.repo.GetAITokenBreakdown(teamUUID, startMs, endMs)
}

func (s *AIService) GetAISecurityMetrics(teamUUID string, startMs, endMs int64) ([]AISecurityMetric, error) {
	return s.repo.GetAISecurityMetrics(teamUUID, startMs, endMs)
}

func (s *AIService) GetAISecurityTimeSeries(teamUUID string, startMs, endMs int64) ([]AISecurityTimeSeries, error) {
	return s.repo.GetAISecurityTimeSeries(teamUUID, startMs, endMs)
}

func (s *AIService) GetAIPiiCategories(teamUUID string, startMs, endMs int64) ([]AIPiiCategory, error) {
	return s.repo.GetAIPiiCategories(teamUUID, startMs, endMs)
}
