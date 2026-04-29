package slowqueries

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
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
	shared.RegisterDualGroup(v1, "/slow-queries", func(g *gin.RouterGroup) {
		g.GET("/patterns", h.GetSlowQueryPatterns)
		g.GET("/collections", h.GetSlowestCollections)
		g.GET("/rate", h.GetSlowQueryRate)
		g.GET("/p99-by-text", h.GetP99ByQueryText)
	})
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &dbSlowModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type dbSlowModule struct {
	handler *Handler
}

func (m *dbSlowModule) Name() string { return "dbSlow" }

func (m *dbSlowModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *dbSlowModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
