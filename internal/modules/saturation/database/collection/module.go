package collection

import (
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
	shared.RegisterDualGroup(v1, "/collection", func(g *gin.RouterGroup) {
		g.GET("/latency", h.GetCollectionLatency)
		g.GET("/ops", h.GetCollectionOps)
		g.GET("/errors", h.GetCollectionErrors)
		g.GET("/query-texts", h.GetCollectionQueryTexts)
		g.GET("/read-vs-write", h.GetCollectionReadVsWrite)
	})
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &dbCollectionModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type dbCollectionModule struct {
	handler *Handler
}

func (m *dbCollectionModule) Name() string                      { return "dbCollection" }
func (m *dbCollectionModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbCollectionModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *dbCollectionModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
