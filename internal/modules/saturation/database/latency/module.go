package latency

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
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
	shared.RegisterDualGroup(v1, "/latency", func(g *gin.RouterGroup) {
		g.GET("/by-system", h.GetLatencyBySystem)
		g.GET("/by-operation", h.GetLatencyByOperation)
		g.GET("/by-collection", h.GetLatencyByCollection)
		g.GET("/by-namespace", h.GetLatencyByNamespace)
		g.GET("/by-server", h.GetLatencyByServer)
		g.GET("/heatmap", h.GetLatencyHeatmap)
	})
}

// NewModule keeps the pre-sketch signature so modules_manifest compiles until
// the sketch querier is wired. Delegates to NewModuleWithSketch with nil.
func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	return NewModuleWithSketch(nativeQuerier, getTenant, nil)
}

func NewModuleWithSketch(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc, sketchQ *sketch.Querier) registry.Module {
	module := &dbLatencyModule{}
	module.configure(nativeQuerier, getTenant, sketchQ)
	return module
}

type dbLatencyModule struct {
	handler *Handler
}

func (m *dbLatencyModule) Name() string                      { return "dbLatency" }
func (m *dbLatencyModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbLatencyModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc, sketchQ *sketch.Querier) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier), sketchQ),
	}
}

func (m *dbLatencyModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
