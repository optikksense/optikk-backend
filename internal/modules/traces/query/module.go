package query

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *TraceHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/traces", h.GetTraces)
	v1.GET("/traces/:traceId/spans", h.GetTraceSpans)
	v1.GET("/spans/:spanId/tree", h.GetSpanTree)
	v1.GET("/spans/search", h.GetSpanSearch)
	v1.GET("/services/:serviceName/errors", h.GetServiceErrors)
	v1.GET("/services/:serviceName/errors/timeseries", h.GetServiceErrorTimeSeries)
	v1.GET("/latency/histogram", h.GetLatencyHistogram)
	v1.GET("/latency/heatmap", h.GetLatencyHeatmap)
	v1.GET("/errors/groups", h.GetErrorGroups)
	v1.GET("/errors/timeseries", h.GetErrorTimeSeries)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &tracesModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type tracesModule struct {
	handler *TraceHandler
}

func (m *tracesModule) Name() string                      { return "traces" }
func (m *tracesModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *tracesModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = NewHandler(
		getTenant,
		NewService(NewRepository(nativeQuerier)),
	)
}

func (m *tracesModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
