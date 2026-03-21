package conversations

import (
	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/registry"
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

func init() {
	registry.Register(&aiConversationsModule{})
}

type aiConversationsModule struct {
	handler *Handler
}

func (m *aiConversationsModule) Name() string                      { return "aiConversations" }
func (m *aiConversationsModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *aiConversationsModule) Init(deps registry.Deps) error {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *aiConversationsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
