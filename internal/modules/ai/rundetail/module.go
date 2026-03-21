package rundetail

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

	v1.GET("/ai/runs/:spanId", h.GetRunDetail)
	v1.GET("/ai/runs/:spanId/messages", h.GetRunMessages)
	v1.GET("/ai/runs/:spanId/context", h.GetRunContext)
}

func init() {
	registry.Register(&aiRunDetailModule{})
}

type aiRunDetailModule struct {
	handler *Handler
}

func (m *aiRunDetailModule) Name() string                      { return "aiRunDetail" }
func (m *aiRunDetailModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *aiRunDetailModule) Init(deps registry.Deps) error {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *aiRunDetailModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
