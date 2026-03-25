package network

import (
	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/database"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/registry"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func NewHandler(db *database.NativeQuerier, getTenant modulecommon.GetTenantFunc) *NetworkHandler {
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
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &networkModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type networkModule struct {
	handler *NetworkHandler
}

func (m *networkModule) Name() string                      { return "network" }
func (m *networkModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *networkModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = NewHandler(nativeQuerier, getTenant)
}

func (m *networkModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
