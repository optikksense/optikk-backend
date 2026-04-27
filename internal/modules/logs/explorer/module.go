// Package explorer owns POST /api/v1/logs/query — the list-first endpoint
// that orchestrates an optional summary/facets/trend include fan-out by
// composing log_facets + log_trends services in-process.
package explorer

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	log_facets "github.com/Optikk-Org/optikk-backend/internal/modules/logs/log_facets"
	log_trends "github.com/Optikk-Org/optikk-backend/internal/modules/logs/log_trends"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config { return Config{Enabled: true} }

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}
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
	facets := log_facets.NewServiceFromDB(db)
	trends := log_trends.NewServiceFromDB(db)
	svc := NewService(repo, facets, trends)
	m.handler = NewHandler(getTenant, svc)
}

func (m *logsExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}

var _ modulecommon.GetTenantFunc = modulecommon.GetTenantFunc(nil)
