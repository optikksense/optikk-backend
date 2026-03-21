package summary

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
	v1.GET("/database/summary", h.GetSummaryStats)
}

func init() {
	registry.Register(&dbSummaryModule{})
}

type dbSummaryModule struct {
	handler *Handler
}

func (m *dbSummaryModule) Name() string                      { return "dbSummary" }
func (m *dbSummaryModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbSummaryModule) Init(deps registry.Deps) error {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *dbSummaryModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
