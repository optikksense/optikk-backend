package dashboard

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

type DashboardService struct{ repo Repository }

func NewService(repo Repository) *DashboardService { return &DashboardService{repo: repo} }

func (s *DashboardService) GetAISummary(teamID int64, startMs, endMs int64) (*AISummary, error) {
	return s.repo.GetAISummary(teamID, startMs, endMs)
}
func (s *DashboardService) GetAIModels(teamID int64, startMs, endMs int64) ([]AIModel, error) {
	return s.repo.GetAIModels(teamID, startMs, endMs)
}
func (s *DashboardService) GetAIPerformanceMetrics(teamID int64, startMs, endMs int64) ([]AIPerformanceMetric, error) {
	return s.repo.GetAIPerformanceMetrics(teamID, startMs, endMs)
}
func (s *DashboardService) GetAIPerformanceTimeSeries(teamID int64, startMs, endMs int64) ([]AIPerformanceTimeSeries, error) {
	return s.repo.GetAIPerformanceTimeSeries(teamID, startMs, endMs)
}
func (s *DashboardService) GetAILatencyHistogram(teamID int64, modelName string, startMs, endMs int64) ([]AILatencyHistogram, error) {
	return s.repo.GetAILatencyHistogram(teamID, modelName, startMs, endMs)
}
func (s *DashboardService) GetAICostMetrics(teamID int64, startMs, endMs int64) ([]AICostMetric, error) {
	return s.repo.GetAICostMetrics(teamID, startMs, endMs)
}
func (s *DashboardService) GetAICostTimeSeries(teamID int64, startMs, endMs int64) ([]AICostTimeSeries, error) {
	return s.repo.GetAICostTimeSeries(teamID, startMs, endMs)
}
func (s *DashboardService) GetAITokenBreakdown(teamID int64, startMs, endMs int64) ([]AITokenBreakdown, error) {
	return s.repo.GetAITokenBreakdown(teamID, startMs, endMs)
}
func (s *DashboardService) GetAISecurityMetrics(teamID int64, startMs, endMs int64) ([]AISecurityMetric, error) {
	return s.repo.GetAISecurityMetrics(teamID, startMs, endMs)
}
func (s *DashboardService) GetAISecurityTimeSeries(teamID int64, startMs, endMs int64) ([]AISecurityTimeSeries, error) {
	return s.repo.GetAISecurityTimeSeries(teamID, startMs, endMs)
}
func (s *DashboardService) GetAIPiiCategories(teamID int64, startMs, endMs int64) ([]AIPiiCategory, error) {
	return s.repo.GetAIPiiCategories(teamID, startMs, endMs)
}
