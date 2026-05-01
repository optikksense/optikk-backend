package slo

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *SLOHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/slo", h.GetSloSli)
	v1.GET("/slo/stats", h.GetSloStats)
	v1.GET("/slo/burn-down", h.GetBurnDown)
	v1.GET("/slo/burn-rate", h.GetBurnRate)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &sloModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type sloModule struct {
	handler *SLOHandler
}

func (m *sloModule) Name() string { return "slo" }

func (m *sloModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &SLOHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *sloModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
