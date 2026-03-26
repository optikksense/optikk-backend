package conversations

import (
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

	v1.GET("/ai/conversations", h.ListConversations)
	v1.GET("/ai/conversations/:conversationId", h.GetConversation)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &aiConversationsModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type aiConversationsModule struct {
	handler *Handler
}

func (m *aiConversationsModule) Name() string                      { return "aiConversations" }
func (m *aiConversationsModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *aiConversationsModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *aiConversationsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
