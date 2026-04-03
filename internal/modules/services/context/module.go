package servicecontext

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	serviceinventory "github.com/Optikk-Org/optikk-backend/internal/modules/services/inventory"
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
	v1.GET("/services/catalog", h.GetServicesCatalog)
	v1.GET("/services/:serviceName/catalog", h.GetServiceCatalog)
	v1.GET("/services/:serviceName/ownership", h.GetServiceOwnership)
	v1.GET("/services/:serviceName/monitors", h.GetServiceMonitors)
	v1.GET("/services/:serviceName/deployments", h.GetServiceDeployments)
	v1.GET("/services/:serviceName/incidents", h.GetServiceIncidents)
	v1.GET("/services/:serviceName/change-events", h.GetServiceChangeEvents)
	v1.GET("/services/:serviceName/scorecard", h.GetServiceScorecard)
}

func NewModule(
	sqlDB *registry.SQLDB,
	nativeQuerier *registry.NativeQuerier,
	getTenant registry.GetTenantFunc,
	appConfig registry.AppConfig,
) registry.Module {
	module := &serviceContextModule{}
	module.configure(sqlDB, nativeQuerier, getTenant, appConfig)
	return module
}

type serviceContextModule struct {
	handler *Handler
}

func (m *serviceContextModule) Name() string                      { return "serviceContext" }
func (m *serviceContextModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *serviceContextModule) configure(
	sqlDB *registry.SQLDB,
	nativeQuerier *registry.NativeQuerier,
	getTenant registry.GetTenantFunc,
	appConfig registry.AppConfig,
) {
	m.handler = NewHandler(
		getTenant,
		NewService(
			NewRepository(sqlDB, appConfig),
			serviceinventory.NewRepository(sqlDB, appConfig),
			NewMetricsRepository(nativeQuerier),
		),
	)
}

func (m *serviceContextModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
