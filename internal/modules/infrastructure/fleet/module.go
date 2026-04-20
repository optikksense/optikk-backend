package fleet

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
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

	v1.GET("/infrastructure/fleet/pods", h.GetFleetPods)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc, sketchQ *sketch.Querier) registry.Module {
	module := &fleetModule{}
	module.configure(nativeQuerier, getTenant, sketchQ)
	return module
}

type fleetModule struct {
	handler *Handler
}

func (m *fleetModule) Name() string                      { return "fleet" }
func (m *fleetModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *fleetModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc, sketchQ *sketch.Querier) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier), sketchQ),
	}
}

func (m *fleetModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
