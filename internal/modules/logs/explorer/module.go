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

func DefaultConfig() Config { return Config{Enabled: true} }

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.POST("/logs/query", h.Query)
	v1.POST("/logs/analytics", h.Analytics)
	v1.GET("/logs/:id", h.GetByID)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	m := &logsExplorerModule{}
	m.configure(nativeQuerier, getTenant)
	return m
}

type logsExplorerModule struct {
	handler *Handler
}

func (m *logsExplorerModule) Name() string                      { return "logsExplorer" }
func (m *logsExplorerModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *logsExplorerModule) configure(db clickhouse.Conn, getTenant registry.GetTenantFunc) {
	repo := NewRepository(db)
	svc := NewService(repo)
	m.handler = NewHandler(getTenant, svc)
}

func (m *logsExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}

var _ modulecommon.GetTenantFunc = modulecommon.GetTenantFunc(nil)
