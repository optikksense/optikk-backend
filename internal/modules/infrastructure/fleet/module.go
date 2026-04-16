package fleet

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

	v1.GET("/infrastructure/fleet/pods", h.GetFleetPods)
}

func NewModule(deps *registry.Deps) (registry.Module, error) {
	module := &fleetModule{}
	module.configure(deps)
	return module, nil
}

type fleetModule struct {
	handler *Handler
}

func (m *fleetModule) Name() string                      { return "fleet" }
func (m *fleetModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *fleetModule) configure(deps *registry.Deps) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
}

func (m *fleetModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
