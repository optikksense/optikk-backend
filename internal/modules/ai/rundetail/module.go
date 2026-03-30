package rundetail

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

	v1.GET("/ai/runs/:spanId", h.GetRunDetail)
	v1.GET("/ai/runs/:spanId/messages", h.GetRunMessages)
	v1.GET("/ai/runs/:spanId/context", h.GetRunContext)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &aiRunDetailModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type aiRunDetailModule struct {
	handler *Handler
}

func (m *aiRunDetailModule) Name() string                      { return "aiRunDetail" }
func (m *aiRunDetailModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *aiRunDetailModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *aiRunDetailModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
