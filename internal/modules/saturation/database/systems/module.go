package systems

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
	shared.RegisterDualGET(v1, "/systems", h.GetDetectedSystems)
}

func NewModule(deps *registry.Deps) (registry.Module, error) {
	module := &dbSystemsModule{}
	module.configure(deps)
	return module, nil
}

type dbSystemsModule struct {
	handler *Handler
}

func (m *dbSystemsModule) Name() string                      { return "dbSystems" }
func (m *dbSystemsModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbSystemsModule) configure(deps *registry.Deps) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
}

func (m *dbSystemsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
