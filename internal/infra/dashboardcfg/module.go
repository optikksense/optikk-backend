package dashboardcfg

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type ModuleConfig struct {
	Enabled bool
}

func DefaultModuleConfig() ModuleConfig {
	return ModuleConfig{Enabled: true}
}

func RegisterRoutes(cfg ModuleConfig, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/default-config/pages", h.ListPages)
	v1.GET("/default-config/pages/:pageId/tabs", h.ListTabs)
	v1.GET("/default-config/pages/:pageId/tabs/:tabId", h.GetTabDocument)
}

func NewModule(
	getTenant registry.GetTenantFunc,
	configRegistry *Registry,
) registry.Module {
	module := &defaultConfigModule{}
	module.configure(getTenant, configRegistry)
	return module
}

type defaultConfigModule struct {
	handler *Handler
}

func (m *defaultConfigModule) Name() string                      { return "defaultConfig" }
func (m *defaultConfigModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *defaultConfigModule) configure(
	getTenant registry.GetTenantFunc,
	configRegistry *Registry,
) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{
			GetTenant: getTenant,
		},
		Service: NewService(configRegistry),
	}
}

func (m *defaultConfigModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultModuleConfig(), group, m.handler)
}
