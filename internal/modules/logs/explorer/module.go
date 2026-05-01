// Package explorer owns POST /api/v1/logs/query — the list-only endpoint.
// Summary / facets / trend are exposed separately at /logs/trends and
// /logs/facets; the frontend fetches them in parallel as needed.
package explorer

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func RegisterRoutes(v1 *gin.RouterGroup, h *Handler) {
	v1.POST("/logs/query", h.Query)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	m := &logsExplorerModule{}
	m.configure(nativeQuerier, getTenant)
	return m
}

type logsExplorerModule struct {
	handler *Handler
}

func (m *logsExplorerModule) Name() string { return "logsExplorer" }

func (m *logsExplorerModule) configure(db clickhouse.Conn, getTenant registry.GetTenantFunc) {
	repo := NewRepository(db)
	svc := NewService(repo)
	m.handler = NewHandler(getTenant, svc)
}

func (m *logsExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(group, m.handler)
}

var _ modulecommon.GetTenantFunc = modulecommon.GetTenantFunc(nil)
