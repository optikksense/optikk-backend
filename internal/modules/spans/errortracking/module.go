package errortracking

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *ErrorTrackingHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/spans/exception-rate-by-type", h.GetExceptionRateByType)
	v1.GET("/spans/error-hotspot", h.GetErrorHotspot)
	v1.GET("/spans/http-5xx-by-route", h.GetHTTP5xxByRoute)
}

func init() {
	registry.Register(&errorTrackingModule{})
}

type errorTrackingModule struct {
	handler *ErrorTrackingHandler
}

func (m *errorTrackingModule) Name() string                      { return "errorTracking" }
func (m *errorTrackingModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *errorTrackingModule) Init(deps registry.Deps) error {
	m.handler = &ErrorTrackingHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *errorTrackingModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
