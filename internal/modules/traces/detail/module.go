package detail

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func RegisterRoutes(v1 *gin.RouterGroup, h *Handler) {
	v1.GET("/traces/:traceId", h.GetTraceSummary)
	v1.GET("/traces/:traceId/span-events", h.GetSpanEvents)
	v1.GET("/traces/:traceId/spans/:spanId/attributes", h.GetSpanAttributes)
	v1.GET("/traces/:traceId/related", h.GetRelatedTraces)
	v1.GET("/traces/:traceId/spans", h.GetTraceSpans)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	m := &tracesDetailModule{}
	m.configure(nativeQuerier, getTenant)
	return m
}

type tracesDetailModule struct {
	handler *Handler
}

func (m *tracesDetailModule) Name() string { return "tracesDetail" }

func (m *tracesDetailModule) configure(db clickhouse.Conn, getTenant registry.GetTenantFunc) {
	repo := NewRepository(db)
	svc := NewService(repo)
	m.handler = NewHandler(getTenant, svc)
}

func (m *tracesDetailModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(group, m.handler)
}
