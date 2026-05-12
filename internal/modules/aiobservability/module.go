package aiobservability

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}

	ai := v1.Group("/ai")

	llm := ai.Group("/llm")
	llm.GET("/request-rate-by-model", h.GetLLMRequestRateByModel)
	llm.GET("/latency-by-model", h.GetLLMLatencyByModel)
	llm.GET("/error-rate-by-model", h.GetLLMErrorRateByModel)
	llm.GET("/token-usage-by-model", h.GetLLMTokenUsageByModel)
	llm.GET("/token-usage-by-provider", h.GetLLMTokenUsageByProvider)
	llm.GET("/cost-by-model", h.GetLLMCostByModel)
	llm.GET("/cost-by-provider", h.GetLLMCostByProvider)
	llm.GET("/top-expensive-calls", h.GetTopExpensiveCalls)
	llm.GET("/top-slow-calls", h.GetTopSlowCalls)

	prompts := ai.Group("/prompts")
	prompts.GET("/usage-by-prompt", h.GetPromptUsageByPrompt)
	prompts.GET("/usage-by-version", h.GetPromptUsageByVersion)
	prompts.GET("/latency-by-version", h.GetPromptLatencyByVersion)
	prompts.GET("/token-usage-by-version", h.GetPromptTokenUsageByVersion)
	prompts.GET("/cost-by-version", h.GetPromptCostByVersion)
	prompts.GET("/traces", h.GetPromptTraces)

	agents := ai.Group("/agents")
	agents.GET("/runs-by-agent", h.GetAgentRunsByAgent)
	agents.GET("/tool-calls-by-tool", h.GetToolCallsByTool)
	agents.GET("/tool-errors-by-tool", h.GetToolErrorsByTool)
	agents.GET("/tool-latency-by-tool", h.GetToolLatencyByTool)

	retrieval := ai.Group("/retrieval")
	retrieval.GET("/request-rate-by-store", h.GetRetrievalRequestRateByStore)
	retrieval.GET("/latency-by-store", h.GetRetrievalLatencyByStore)
	retrieval.GET("/errors-by-store", h.GetRetrievalErrorsByStore)

	ai.GET("/traces/query", h.QueryTraces)
	ai.GET("/facets", h.GetFacets)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &aiObservabilityModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type aiObservabilityModule struct {
	handler *Handler
}

func (m *aiObservabilityModule) Name() string { return "ai_observability" }

func (m *aiObservabilityModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *aiObservabilityModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
