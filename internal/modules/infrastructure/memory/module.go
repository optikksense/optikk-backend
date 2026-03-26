package memory

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

func NewHandler(db *dbutil.NativeQuerier, getTenant modulecommon.GetTenantFunc) *MemoryHandler {
	return &MemoryHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(db)),
	}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *MemoryHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/infrastructure/memory")
	g.GET("/usage", h.GetMemoryUsage)
	g.GET("/usage-percentage", h.GetMemoryUsagePercentage)
	g.GET("/swap", h.GetSwapUsage)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &memoryModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type memoryModule struct {
	handler *MemoryHandler
}

func (m *memoryModule) Name() string                      { return "memory" }
func (m *memoryModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *memoryModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = NewHandler(nativeQuerier, getTenant)
}

func (m *memoryModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
