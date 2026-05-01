package errors

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
	shared.RegisterGroup(v1, "/errors", func(g *gin.RouterGroup) {
		g.GET("/by-system", h.GetErrorsBySystem)
		g.GET("/by-operation", h.GetErrorsByOperation)
		g.GET("/by-error-type", h.GetErrorsByErrorType)
		g.GET("/by-collection", h.GetErrorsByCollection)
		g.GET("/by-status", h.GetErrorsByResponseStatus)
		g.GET("/ratio", h.GetErrorRatio)
	})
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &dbErrorsModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type dbErrorsModule struct {
	handler *Handler
}

func (m *dbErrorsModule) Name() string { return "dbErrors" }

func (m *dbErrorsModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *dbErrorsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
