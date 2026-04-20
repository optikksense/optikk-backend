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
	v1.POST("/traces/explorer/query", h.Query)
	v1.POST("/explorer/traces/analytics", h.Analytics)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &tracesExplorerModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type tracesExplorerModule struct {
	handler *Handler
}

func (m *tracesExplorerModule) Name() string                      { return "tracesExplorer" }
func (m *tracesExplorerModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *tracesExplorerModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	// sketchQ is intentionally nil here: traces/explorer re-uses the base
	// service only for keyset search + facets, none of which read p95. When
	// the manifest wires sketchQ through, switch to a WithSketch variant.
	traceService := spantraces.NewService(spantraces.NewRepository(nativeQuerier), nil)
	m.handler = NewHandler(getTenant, NewService(traceService), nativeQuerier)
}

func (m *tracesExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
