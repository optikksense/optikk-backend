package volume

import (
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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/database/ops")
	g.GET("/by-system", h.GetOpsBySystem)
	g.GET("/by-operation", h.GetOpsByOperation)
	g.GET("/by-collection", h.GetOpsByCollection)
	g.GET("/read-vs-write", h.GetReadVsWrite)
	g.GET("/by-namespace", h.GetOpsByNamespace)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &dbVolumeModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type dbVolumeModule struct {
	handler *Handler
}

func (m *dbVolumeModule) Name() string                      { return "dbVolume" }
func (m *dbVolumeModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbVolumeModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *dbVolumeModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
