package paths

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func RegisterRoutes(v1 *gin.RouterGroup, h *Handler) {
	v1.GET("/traces/:traceId/critical-path", h.GetCriticalPath)
	v1.GET("/traces/:traceId/error-path", h.GetErrorPath)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	m := &tracesPathsModule{}
	m.configure(nativeQuerier, getTenant)
	return m
}

type tracesPathsModule struct {
	handler *Handler
}

func (m *tracesPathsModule) Name() string { return "tracesPaths" }

func (m *tracesPathsModule) configure(db clickhouse.Conn, getTenant registry.GetTenantFunc) {
	repo := NewRepository(db)
	svc := NewService(repo)
	m.handler = NewHandler(getTenant, svc)
}

func (m *tracesPathsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(group, m.handler)
}
