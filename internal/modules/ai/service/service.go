package service

import (
	"github.com/observability/observability-backend-go/internal/modules/ai/model"
	"github.com/observability/observability-backend-go/internal/modules/ai/store"
)

// Service encapsulates the business logic for AI models.
type Service interface {
	GetAISummary(teamUUID string, startMs, endMs int64) (*model.AISummary, error)
	GetAIModels(teamUUID string, startMs, endMs int64) ([]model.AIModel, error)
	GetAIPerformanceMetrics(teamUUID string, startMs, endMs int64) ([]model.AIPerformanceMetric, error)
	GetAIPerformanceTimeSeries(teamUUID string, startMs, endMs int64) ([]model.AIPerformanceTimeSeries, error)
	GetAILatencyHistogram(teamUUID string, modelName string, startMs, endMs int64) ([]model.AILatencyHistogram, error)
	GetAICostMetrics(teamUUID string, startMs, endMs int64) ([]model.AICostMetric, error)
	GetAICostTimeSeries(teamUUID string, startMs, endMs int64) ([]model.AICostTimeSeries, error)
	GetAITokenBreakdown(teamUUID string, startMs, endMs int64) ([]model.AITokenBreakdown, error)
	GetAISecurityMetrics(teamUUID string, startMs, endMs int64) ([]model.AISecurityMetric, error)
	GetAISecurityTimeSeries(teamUUID string, startMs, endMs int64) ([]model.AISecurityTimeSeries, error)
	GetAIPiiCategories(teamUUID string, startMs, endMs int64) ([]model.AIPiiCategory, error)
}

// AIService provides business logic orchestration for AI models.
type AIService struct {
	repo store.Repository
}

// NewService creates a new AIService.
func NewService(repo store.Repository) *AIService {
	return &AIService{repo: repo}
}

func (s *AIService) GetAISummary(teamUUID string, startMs, endMs int64) (*model.AISummary, error) {
	return s.repo.GetAISummary(teamUUID, startMs, endMs)
}

func (s *AIService) GetAIModels(teamUUID string, startMs, endMs int64) ([]model.AIModel, error) {
	return s.repo.GetAIModels(teamUUID, startMs, endMs)
}

func (s *AIService) GetAIPerformanceMetrics(teamUUID string, startMs, endMs int64) ([]model.AIPerformanceMetric, error) {
	return s.repo.GetAIPerformanceMetrics(teamUUID, startMs, endMs)
}

func (s *AIService) GetAIPerformanceTimeSeries(teamUUID string, startMs, endMs int64) ([]model.AIPerformanceTimeSeries, error) {
	return s.repo.GetAIPerformanceTimeSeries(teamUUID, startMs, endMs)
}

func (s *AIService) GetAILatencyHistogram(teamUUID string, modelName string, startMs, endMs int64) ([]model.AILatencyHistogram, error) {
	return s.repo.GetAILatencyHistogram(teamUUID, modelName, startMs, endMs)
}

func (s *AIService) GetAICostMetrics(teamUUID string, startMs, endMs int64) ([]model.AICostMetric, error) {
	return s.repo.GetAICostMetrics(teamUUID, startMs, endMs)
}

func (s *AIService) GetAICostTimeSeries(teamUUID string, startMs, endMs int64) ([]model.AICostTimeSeries, error) {
	return s.repo.GetAICostTimeSeries(teamUUID, startMs, endMs)
}

func (s *AIService) GetAITokenBreakdown(teamUUID string, startMs, endMs int64) ([]model.AITokenBreakdown, error) {
	return s.repo.GetAITokenBreakdown(teamUUID, startMs, endMs)
}

func (s *AIService) GetAISecurityMetrics(teamUUID string, startMs, endMs int64) ([]model.AISecurityMetric, error) {
	return s.repo.GetAISecurityMetrics(teamUUID, startMs, endMs)
}

func (s *AIService) GetAISecurityTimeSeries(teamUUID string, startMs, endMs int64) ([]model.AISecurityTimeSeries, error) {
	return s.repo.GetAISecurityTimeSeries(teamUUID, startMs, endMs)
}

func (s *AIService) GetAIPiiCategories(teamUUID string, startMs, endMs int64) ([]model.AIPiiCategory, error) {
	return s.repo.GetAIPiiCategories(teamUUID, startMs, endMs)
}
