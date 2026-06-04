package cpu

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

<<<<<<< HEAD

=======
>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326
type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func NewHandler(db clickhouse.Conn, getTenant modulecommon.GetTenantFunc) *CPUHandler {
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
	g.GET("/avg", h.GetAvgCPU)
	g.GET("/by-instance", h.GetCPUByInstance)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &cpuModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type cpuModule struct {
	handler *CPUHandler
}

func (m *cpuModule) Name() string { return "cpu" }

func (m *cpuModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = NewHandler(nativeQuerier, getTenant)
}

func (m *cpuModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
