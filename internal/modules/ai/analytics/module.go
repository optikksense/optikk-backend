package analytics

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// NewModule creates the AI analytics registry.Module.
func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	m := &aiAnalyticsModule{}
	m.configure(nativeQuerier, getTenant)
	return m
}

type aiAnalyticsModule struct {
	handler *Handler
}

func (m *aiAnalyticsModule) Name() string                      { return "aiAnalytics" }
func (m *aiAnalyticsModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *aiAnalyticsModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *aiAnalyticsModule) RegisterRoutes(group *gin.RouterGroup) {
	// Model Catalog & Comparison
	models := group.Group("/ai/analytics")
	models.GET("/model-catalog", m.handler.GetModelCatalog)
	models.GET("/model-comparison", m.handler.GetModelComparison)
	models.GET("/model-timeseries/:model", m.handler.GetModelTimeseries)
	models.GET("/latency-distribution", m.handler.GetLatencyDistribution)
	models.GET("/parameter-impact", m.handler.GetParameterImpact)

	// Cost
	models.GET("/cost-summary", m.handler.GetCostSummary)
	models.GET("/cost-timeseries", m.handler.GetCostTimeseries)
	models.GET("/token-economics", m.handler.GetTokenEconomics)

	// Errors
	models.GET("/error-patterns", m.handler.GetErrorPatterns)
	models.GET("/error-timeseries", m.handler.GetErrorTimeseries)
	models.GET("/finish-reason-analysis", m.handler.GetFinishReasonTrends)

	// Conversations
	models.GET("/conversations", m.handler.GetConversations)
	models.GET("/conversations/:id", m.handler.GetConversationTurns)
	models.GET("/conversations/:id/summary", m.handler.GetConversationSummary)
}
