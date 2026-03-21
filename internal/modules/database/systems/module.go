package systems

import (
	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/registry"
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
	v1.GET("/database/systems", h.GetDetectedSystems)
}

func init() {
	registry.Register(&dbSystemsModule{})
}

type dbSystemsModule struct {
	handler *Handler
}

func (m *dbSystemsModule) Name() string                      { return "dbSystems" }
func (m *dbSystemsModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbSystemsModule) Init(deps registry.Deps) error {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *dbSystemsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
