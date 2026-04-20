package systems

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
	shared.RegisterDualGET(v1, "/systems", h.GetDetectedSystems)
}

// NewModule keeps the pre-sketch signature so modules_manifest compiles until
// the sketch querier is wired. Internally delegates to NewModuleWithSketch
// with a nil sketchQ (service falls back to raw sum/count avg only).
func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	return NewModuleWithSketch(nativeQuerier, getTenant, nil)
}

// NewModuleWithSketch is the sketch-aware constructor. modules_manifest will
// switch to this once phase-2 wiring lands.
func NewModuleWithSketch(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc, sketchQ *sketch.Querier) registry.Module {
	module := &dbSystemsModule{}
	module.configure(nativeQuerier, getTenant, sketchQ)
	return module
}

type dbSystemsModule struct {
	handler *Handler
}

func (m *dbSystemsModule) Name() string                      { return "dbSystems" }
func (m *dbSystemsModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbSystemsModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc, sketchQ *sketch.Querier) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier), sketchQ),
	}
}

func (m *dbSystemsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
