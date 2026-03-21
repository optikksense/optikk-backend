package analytics

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
	v1.GET("/logs/histogram", h.GetLogHistogram)
	v1.GET("/logs/volume", h.GetLogVolume)
	v1.GET("/logs/stats", h.GetLogStats)
	v1.GET("/logs/fields", h.GetLogFields)
	v1.GET("/logs/aggregate", h.GetLogAggregate)
}

func init() {
	registry.Register(&logAnalyticsModule{})
}

type logAnalyticsModule struct {
	handler *Handler
}

func (m *logAnalyticsModule) Name() string                      { return "logAnalytics" }
func (m *logAnalyticsModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *logAnalyticsModule) Init(deps registry.Deps) error {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *logAnalyticsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
