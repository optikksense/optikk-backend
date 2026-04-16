package network

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func NewHandler(db *dbutil.NativeQuerier, getTenant modulecommon.GetTenantFunc) *NetworkHandler {
	return &NetworkHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(db)),
	}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *NetworkHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/infrastructure/network")
	g.GET("/io", h.GetNetworkIO)
	g.GET("/packets", h.GetNetworkPackets)
	g.GET("/errors", h.GetNetworkErrors)
	g.GET("/dropped", h.GetNetworkDropped)
	g.GET("/connections", h.GetNetworkConnections)
	g.GET("/avg", h.GetAvgNetwork)
	g.GET("/by-service", h.GetNetworkByService)
	g.GET("/by-instance", h.GetNetworkByInstance)
}

func NewModule(deps *registry.Deps) (registry.Module, error) {
	module := &networkModule{}
	module.configure(deps)
	return module, nil
}

type networkModule struct {
	handler *NetworkHandler
}

func (m *networkModule) Name() string                      { return "network" }
func (m *networkModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *networkModule) configure(deps *registry.Deps) {
	m.handler = NewHandler(deps.NativeQuerier, deps.GetTenant)
}

func (m *networkModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
