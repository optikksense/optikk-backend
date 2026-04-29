package resourceutil //nolint:misspell

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/connpool"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/cpu"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/disk"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/memory"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/network"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *ResourceUtilisationHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	g := v1.Group("/infrastructure/resource-utilisation")
	g.GET("/avg-cpu", h.GetAvgCPU)
	g.GET("/avg-memory", h.GetAvgMemory)
	g.GET("/avg-network", h.GetAvgNetwork)
	g.GET("/avg-conn-pool", h.GetAvgConnPool)
	g.GET("/cpu-usage-percentage", h.GetCPUUsagePercentage)
	g.GET("/memory-usage-percentage", h.GetMemoryUsagePercentage)
	g.GET("/by-service", h.GetByService)
	g.GET("/by-instance", h.GetByInstance)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &resourceUtilisationModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type resourceUtilisationModule struct {
	handler *ResourceUtilisationHandler
}

func (m *resourceUtilisationModule) Name() string { return "resourceUtilisation" }

func (m *resourceUtilisationModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	repo := NewRepository(
		nativeQuerier,
		cpu.NewService(cpu.NewRepository(nativeQuerier)),
		memory.NewService(memory.NewRepository(nativeQuerier)),
		disk.NewService(disk.NewRepository(nativeQuerier)),
		network.NewService(network.NewRepository(nativeQuerier)),
		connpool.NewService(connpool.NewRepository(nativeQuerier)),
	)
	m.handler = &ResourceUtilisationHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(repo),
	}
}

func (m *resourceUtilisationModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
