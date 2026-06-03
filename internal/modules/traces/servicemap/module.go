package servicemap

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func RegisterRoutes(v1 *gin.RouterGroup, h *Handler) {
	v1.GET("/traces/:traceId/service-map", h.GetServiceMap)
	v1.GET("/traces/:traceId/errors", h.GetTraceErrors)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	m := &tracesServicemapModule{}
	m.configure(nativeQuerier, getTenant)
	return m
}

type tracesServicemapModule struct {
	handler *Handler
}

func (m *tracesServicemapModule) Name() string { return "tracesServicemap" }

func (m *tracesServicemapModule) configure(db clickhouse.Conn, getTenant registry.GetTenantFunc) {
	repo := NewRepository(db)
	svc := NewService(repo)
	m.handler = NewHandler(getTenant, svc)
}

func (m *tracesServicemapModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(group, m.handler)
}
