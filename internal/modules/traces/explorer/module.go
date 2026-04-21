package explorer

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	spantraces "github.com/Optikk-Org/optikk-backend/internal/modules/traces/query"
	"github.com/gin-gonic/gin"
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
	v1.POST("/traces/query", h.Query)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &tracesExplorerModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type tracesExplorerModule struct {
	handler *Handler
}

func (m *tracesExplorerModule) Name() string                      { return "tracesHub" }
func (m *tracesExplorerModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *tracesExplorerModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	traceService := spantraces.NewService(spantraces.NewRepository(nativeQuerier))
	m.handler = NewHandler(getTenant, NewService(traceService))
}

func (m *tracesExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
