package detail

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
	v1.GET("/logs/surrounding", h.GetLogSurrounding)
	v1.GET("/logs/detail", h.GetLogDetail)
}

func init() {
	registry.Register(&logDetailModule{})
}

type logDetailModule struct {
	handler *Handler
}

func (m *logDetailModule) Name() string                      { return "logDetail" }
func (m *logDetailModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *logDetailModule) Init(deps registry.Deps) error {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *logDetailModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
