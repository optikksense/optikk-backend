package ai

type Service interface {
	GetAISummary(teamID int64, startMs, endMs int64) (*AISummary, error)
	GetAIModels(teamID int64, startMs, endMs int64) ([]AIModel, error)
	GetAIPerformanceMetrics(teamID int64, startMs, endMs int64) ([]AIPerformanceMetric, error)
	GetAIPerformanceTimeSeries(teamID int64, startMs, endMs int64) ([]AIPerformanceTimeSeries, error)
	GetAILatencyHistogram(teamID int64, modelName string, startMs, endMs int64) ([]AILatencyHistogram, error)
	GetAICostMetrics(teamID int64, startMs, endMs int64) ([]AICostMetric, error)
	GetAICostTimeSeries(teamID int64, startMs, endMs int64) ([]AICostTimeSeries, error)
	GetAITokenBreakdown(teamID int64, startMs, endMs int64) ([]AITokenBreakdown, error)
	GetAISecurityMetrics(teamID int64, startMs, endMs int64) ([]AISecurityMetric, error)
	GetAISecurityTimeSeries(teamID int64, startMs, endMs int64) ([]AISecurityTimeSeries, error)
	GetAIPiiCategories(teamID int64, startMs, endMs int64) ([]AIPiiCategory, error)
}

type AIService struct {
	repo Repository
}

func NewService(repo Repository) *AIService {
	return &AIService{repo: repo}
}

func (s *AIService) GetAISummary(teamID int64, startMs, endMs int64) (*AISummary, error) {
	return s.repo.GetAISummary(teamID, startMs, endMs)
}

func (s *AIService) GetAIModels(teamID int64, startMs, endMs int64) ([]AIModel, error) {
	return s.repo.GetAIModels(teamID, startMs, endMs)
}

func (s *AIService) GetAIPerformanceMetrics(teamID int64, startMs, endMs int64) ([]AIPerformanceMetric, error) {
	return s.repo.GetAIPerformanceMetrics(teamID, startMs, endMs)
}

func (s *AIService) GetAIPerformanceTimeSeries(teamID int64, startMs, endMs int64) ([]AIPerformanceTimeSeries, error) {
	return s.repo.GetAIPerformanceTimeSeries(teamID, startMs, endMs)
}

func (s *AIService) GetAILatencyHistogram(teamID int64, modelName string, startMs, endMs int64) ([]AILatencyHistogram, error) {
	return s.repo.GetAILatencyHistogram(teamID, modelName, startMs, endMs)
}

func (s *AIService) GetAICostMetrics(teamID int64, startMs, endMs int64) ([]AICostMetric, error) {
	return s.repo.GetAICostMetrics(teamID, startMs, endMs)
}

func (s *AIService) GetAICostTimeSeries(teamID int64, startMs, endMs int64) ([]AICostTimeSeries, error) {
	return s.repo.GetAICostTimeSeries(teamID, startMs, endMs)
}

func (s *AIService) GetAITokenBreakdown(teamID int64, startMs, endMs int64) ([]AITokenBreakdown, error) {
	return s.repo.GetAITokenBreakdown(teamID, startMs, endMs)
}

func (s *AIService) GetAISecurityMetrics(teamID int64, startMs, endMs int64) ([]AISecurityMetric, error) {
	return s.repo.GetAISecurityMetrics(teamID, startMs, endMs)
}

func (s *AIService) GetAISecurityTimeSeries(teamID int64, startMs, endMs int64) ([]AISecurityTimeSeries, error) {
	return s.repo.GetAISecurityTimeSeries(teamID, startMs, endMs)
}

func (s *AIService) GetAIPiiCategories(teamID int64, startMs, endMs int64) ([]AIPiiCategory, error) {
	return s.repo.GetAIPiiCategories(teamID, startMs, endMs)
}
