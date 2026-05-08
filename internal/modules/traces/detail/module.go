package detail

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
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
	v1.GET("/traces/:traceId", h.GetTraceSummary)
	v1.GET("/traces/:traceId/span-events", h.GetSpanEvents)
	v1.GET("/traces/:traceId/spans/:spanId/attributes", h.GetSpanAttributes)
	v1.GET("/traces/:traceId/related", h.GetRelatedTraces)
	v1.GET("/traces/:traceId/spans", h.GetTraceSpans)
	v1.GET("/spans/:spanId/tree", h.GetSpanTree)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &detailModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type detailModule struct {
	handler *Handler
}

func (m *detailModule) Name() string { return "traceDetail" }

func (m *detailModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	svc := NewService(NewRepository(nativeQuerier))
	m.handler = &Handler{DBTenant: modulecommon.DBTenant{GetTenant: getTenant}, Service: svc}
}

func (m *detailModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
