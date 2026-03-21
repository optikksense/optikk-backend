package cpu

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

func NewHandler(db *database.NativeQuerier, getTenant modulecommon.GetTenantFunc) *CPUHandler {
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
}

func init() {
	registry.Register(&cpuModule{})
}

type cpuModule struct {
	handler *CPUHandler
}

func (m *cpuModule) Name() string                      { return "cpu" }
func (m *cpuModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *cpuModule) Init(deps registry.Deps) error {
	m.handler = NewHandler(deps.NativeQuerier, deps.GetTenant)
	return nil
}

func (m *cpuModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
