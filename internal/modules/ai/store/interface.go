package store

import "github.com/observability/observability-backend-go/internal/modules/ai/model"

// Repository encapsulates data access logic for AI models.
type Repository interface {
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
