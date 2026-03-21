package analytics

import (
	"github.com/gin-gonic/gin"
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

	v1.POST("/spans/analytics", h.PostAnalytics)
	v1.GET("/spans/analytics/dimensions", h.GetDimensions)
}

func init() {
	registry.Register(&spanAnalyticsModule{})
}

type spanAnalyticsModule struct {
	handler *Handler
}

func (m *spanAnalyticsModule) Name() string                      { return "spanAnalytics" }
func (m *spanAnalyticsModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *spanAnalyticsModule) Init(deps registry.Deps) error {
	m.handler = NewHandler(
		deps.GetTenant,
		NewService(NewRepository(deps.NativeQuerier)),
	)
	return nil
}

func (m *spanAnalyticsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
