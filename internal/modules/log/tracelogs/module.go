package tracelogs

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
	v1.GET("/traces/:traceId/logs", h.GetTraceLogs)
}

func init() {
	registry.Register(&logTraceLogsModule{})
}

type logTraceLogsModule struct {
	handler *Handler
}

func (m *logTraceLogsModule) Name() string                      { return "logTraceLogs" }
func (m *logTraceLogsModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *logTraceLogsModule) Init(deps registry.Deps) error {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *logTraceLogsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
