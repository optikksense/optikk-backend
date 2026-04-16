package volume

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
	shared.RegisterDualGroup(v1, "/ops", func(g *gin.RouterGroup) {
		g.GET("/by-system", h.GetOpsBySystem)
		g.GET("/by-operation", h.GetOpsByOperation)
		g.GET("/by-collection", h.GetOpsByCollection)
		g.GET("/read-vs-write", h.GetReadVsWrite)
		g.GET("/by-namespace", h.GetOpsByNamespace)
	})
}

func NewModule(deps *registry.Deps) (registry.Module, error) {
	module := &dbVolumeModule{}
	module.configure(deps)
	return module, nil
}

type dbVolumeModule struct {
	handler *Handler
}

func (m *dbVolumeModule) Name() string                      { return "dbVolume" }
func (m *dbVolumeModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbVolumeModule) configure(deps *registry.Deps) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
}

func (m *dbVolumeModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
