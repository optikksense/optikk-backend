package trace_analytics //nolint:revive,stylecheck

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct{ Enabled bool }

func DefaultConfig() Config { return Config{Enabled: true} }

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.POST("/traces/analytics", h.Analytics)
}

func NewModule(db clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	m := &module{}
	m.configure(db, getTenant)
	return m
}

type module struct {
	handler *Handler
}

func (m *module) Name() string                      { return "traceAnalytics" }

func (m *module) configure(db clickhouse.Conn, getTenant registry.GetTenantFunc) {
	repo := NewRepository(db)
	svc := NewService(repo)
	m.handler = NewHandler(getTenant, svc)
}

func (m *module) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}

var _ modulecommon.GetTenantFunc = modulecommon.GetTenantFunc(nil)
