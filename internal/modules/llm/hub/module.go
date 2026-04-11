package hub

import (
	"database/sql"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// NewModule registers LLM hub HTTP routes backed by MySQL.
func NewModule(db *sql.DB, getTenant registry.GetTenantFunc) registry.Module {
	repo := NewRepository(db)
	svc := NewService(repo)
	h := NewHandler(modulecommon.DBTenant{DB: db, GetTenant: getTenant}, svc)
	return &llmHubModule{handler: h}
}

type llmHubModule struct {
	handler *Handler
}

func (m *llmHubModule) Name() string                      { return "llmHub" }
func (m *llmHubModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *llmHubModule) RegisterRoutes(g *gin.RouterGroup) {
	if m.handler == nil {
		return
	}
	h := m.handler
	g.POST("/ai/llm/scores", h.PostScore)
	g.POST("/ai/llm/scores/batch", h.PostScoresBatch)
	g.GET("/ai/llm/scores", h.ListScores)
	g.GET("/ai/llm/prompts", h.ListPrompts)
	g.POST("/ai/llm/prompts", h.CreatePrompt)
	g.PATCH("/ai/llm/prompts/:id", h.UpdatePrompt)
	g.DELETE("/ai/llm/prompts/:id", h.DeletePrompt)
	g.GET("/ai/llm/datasets", h.ListDatasets)
	g.POST("/ai/llm/datasets", h.CreateDataset)
	g.GET("/ai/llm/datasets/:id", h.GetDataset)
	g.GET("/ai/llm/settings", h.GetSettings)
	g.PATCH("/ai/llm/settings", h.PatchSettings)
}
