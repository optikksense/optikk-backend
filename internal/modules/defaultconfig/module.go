package defaultconfig

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

	v1.GET("/default-config/pages", h.ListPages)
	v1.GET("/default-config/pages/:pageId/tabs", h.ListTabs)
	v1.GET("/default-config/pages/:pageId/tabs/:tabId/components", h.ListComponents)
	v1.PUT("/default-config/pages/:pageId", h.SavePageOverride)
}

func init() {
	registry.Register(&defaultConfigModule{})
}

type defaultConfigModule struct {
	handler *Handler
}

func (m *defaultConfigModule) Name() string                      { return "defaultConfig" }
func (m *defaultConfigModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *defaultConfigModule) Init(deps registry.Deps) error {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{
			DB:        deps.DB,
			GetTenant: deps.GetTenant,
		},
		Service: NewService(NewRepository(deps.DB), deps.ConfigRegistry, deps.Config.App.DashboardConfigUseDefaults),
	}
	return nil
}

func (m *defaultConfigModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
