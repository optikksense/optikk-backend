package explorer

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func RegisterRoutes(v1 *gin.RouterGroup, h *Handler) {
	v1.POST("/traces/query", h.Query)
	v1.POST("/traces/facets", h.QueryFacets)
	v1.POST("/traces/trend", h.QueryTrend)
	v1.POST("/traces/suggest", h.Suggest)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	m := &tracesExplorerModule{}
	m.configure(nativeQuerier, getTenant)
	return m
}

type tracesExplorerModule struct {
	handler *Handler
}

func (m *tracesExplorerModule) Name() string { return "tracesExplorer" }

func (m *tracesExplorerModule) configure(db clickhouse.Conn, getTenant registry.GetTenantFunc) {
	repo := NewRepository(db)
	svc := NewService(repo)
	m.handler = NewHandler(getTenant, svc)
}

func (m *tracesExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(group, m.handler)
}
