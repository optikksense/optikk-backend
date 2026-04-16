package cpu

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

func NewHandler(db *dbutil.NativeQuerier, getTenant modulecommon.GetTenantFunc) *CPUHandler {
	return &CPUHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(db)),
	}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *CPUHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/infrastructure/cpu")
	g.GET("/time", h.GetCPUTime)
	g.GET("/usage-percentage", h.GetCPUUsagePercentage)
	g.GET("/load-average", h.GetLoadAverage)
	g.GET("/process-count", h.GetProcessCount)
	g.GET("/avg", h.GetAvgCPU)
	g.GET("/by-service", h.GetCPUByService)
	g.GET("/by-instance", h.GetCPUByInstance)
}

func NewModule(deps *registry.Deps) (registry.Module, error) {
	module := &cpuModule{}
	module.configure(deps)
	return module, nil
}

type cpuModule struct {
	handler *CPUHandler
}

func (m *cpuModule) Name() string                      { return "cpu" }
func (m *cpuModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *cpuModule) configure(deps *registry.Deps) {
	m.handler = NewHandler(deps.NativeQuerier, deps.GetTenant)
}

func (m *cpuModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
