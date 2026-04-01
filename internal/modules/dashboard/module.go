package dashboard

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

	v1.GET("/default-config/pages", h.ListPages)
	v1.GET("/default-config/pages/:pageId/tabs", h.ListTabs)
	v1.GET("/default-config/pages/:pageId/tabs/:tabId", h.GetTabDocument)
}

func NewModule(
	sqlDB *registry.SQLDB,
	getTenant registry.GetTenantFunc,
	appConfig registry.AppConfig,
	configRegistry *registry.ConfigRegistry,
) registry.Module {
	module := &defaultConfigModule{}
	module.configure(sqlDB, getTenant, appConfig, configRegistry)
	return module
}

type defaultConfigModule struct {
	handler *Handler
}

func (m *defaultConfigModule) Name() string                      { return "defaultConfig" }
func (m *defaultConfigModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *defaultConfigModule) configure(
	sqlDB *registry.SQLDB,
	getTenant registry.GetTenantFunc,
	appConfig registry.AppConfig,
	configRegistry *registry.ConfigRegistry,
) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{
			DB:        sqlDB,
			GetTenant: getTenant,
		},
		Service: NewService(NewRepository(sqlDB, appConfig), configRegistry, appConfig.App.DashboardConfigUseDefaults),
	}
}

func (m *defaultConfigModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
