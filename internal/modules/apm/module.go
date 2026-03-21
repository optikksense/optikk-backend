package apm

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *APMHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/apm")
	g.GET("/rpc-duration", h.GetRPCDuration)
	g.GET("/rpc-request-rate", h.GetRPCRequestRate)
	g.GET("/messaging-publish-duration", h.GetMessagingPublishDuration)
	g.GET("/process-cpu", h.GetProcessCPU)
	g.GET("/process-memory", h.GetProcessMemory)
	g.GET("/open-fds", h.GetOpenFDs)
	g.GET("/uptime", h.GetUptime)
}

func init() {
	registry.Register(&apmModule{})
}

type apmModule struct {
	handler *APMHandler
}

func (m *apmModule) Name() string                      { return "apm" }
func (m *apmModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *apmModule) Init(deps registry.Deps) error {
	m.handler = &APMHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *apmModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
