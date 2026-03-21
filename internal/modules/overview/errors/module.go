package errors

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *ErrorHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/overview/errors/service-error-rate", h.GetServiceErrorRate)
	v1.GET("/overview/errors/error-volume", h.GetErrorVolume)
	v1.GET("/overview/errors/latency-during-error-windows", h.GetLatencyDuringErrorWindows)
	v1.GET("/overview/errors/groups", h.GetErrorGroups)
	v1.GET("/errors/groups/:groupId", h.GetErrorGroupDetail)
	v1.GET("/errors/groups/:groupId/traces", h.GetErrorGroupTraces)
	v1.GET("/errors/groups/:groupId/timeseries", h.GetErrorGroupTimeseries)
}

func init() {
	registry.Register(&overviewErrorsModule{})
}

type overviewErrorsModule struct {
	handler *ErrorHandler
}

func (m *overviewErrorsModule) Name() string                      { return "overviewErrors" }
func (m *overviewErrorsModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *overviewErrorsModule) Init(deps registry.Deps) error {
	m.handler = &ErrorHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *overviewErrorsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
