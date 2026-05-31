package traces

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

type tracesModule struct {
	handler *Handler
}

func NewModule(db clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	m := &tracesModule{}
	m.configure(db, getTenant)
	return m
}

func (m *tracesModule) Name() string { return "traces" }

func (m *tracesModule) configure(db clickhouse.Conn, getTenant registry.GetTenantFunc) {
	repo := NewRepository(db)
	svc := NewService(repo)
	m.handler = NewHandler(getTenant, svc)
}

func (m *tracesModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.POST("/traces/query", h.Query)
	v1.POST("/traces/facets", h.QueryFacets)
	v1.POST("/traces/trend", h.QueryTrend)
	v1.POST("/traces/suggest", h.Suggest)

	v1.GET("/traces/:traceId", h.GetTraceSummary)
	v1.GET("/traces/:traceId/span-events", h.GetSpanEvents)
	v1.GET("/traces/:traceId/spans/:spanId/attributes", h.GetSpanAttributes)
	v1.GET("/traces/:traceId/related", h.GetRelatedTraces)
	v1.GET("/traces/:traceId/spans", h.GetTraceSpans)
	v1.GET("/traces/:traceId/critical-path", h.GetCriticalPath)
	v1.GET("/traces/:traceId/error-path", h.GetErrorPath)
	v1.GET("/traces/:traceId/service-map", h.GetServiceMap)
	v1.GET("/traces/:traceId/errors", h.GetTraceErrors)
}
