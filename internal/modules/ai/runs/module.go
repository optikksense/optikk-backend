package runs

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

	v1.GET("/ai/runs", h.ListRuns)
	v1.GET("/ai/runs/summary", h.GetRunsSummary)
	v1.GET("/ai/runs/models", h.ListModels)
	v1.GET("/ai/runs/operations", h.ListOperations)
}

func init() {
	registry.Register(&aiRunsModule{})
}

type aiRunsModule struct {
	handler *Handler
}

func (m *aiRunsModule) Name() string                      { return "aiRuns" }
func (m *aiRunsModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *aiRunsModule) Init(deps registry.Deps) error {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *aiRunsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
