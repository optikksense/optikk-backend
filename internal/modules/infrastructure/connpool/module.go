package connpool

import (
	"github.com/ClickHouse/clickhouse-go/v2"
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

func NewHandler(db clickhouse.Conn, getTenant modulecommon.GetTenantFunc) *ConnPoolHandler {
	return &ConnPoolHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(db)),
	}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *ConnPoolHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/infrastructure/connpool")
	g.GET("/avg", h.GetAvgConnPool)
	g.GET("/by-service", h.GetConnPoolByService)
	g.GET("/by-instance", h.GetConnPoolByInstance)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &connpoolModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type connpoolModule struct {
	handler *ConnPoolHandler
}

func (m *connpoolModule) Name() string                      { return "connpool" }
func (m *connpoolModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *connpoolModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = NewHandler(nativeQuerier, getTenant)
}

func (m *connpoolModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
