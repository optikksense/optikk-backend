package latency

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
	v1.GET("/latency/histogram", h.Histogram)
	v1.GET("/latency/heatmap", h.Heatmap)
}

func NewModule(db clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	m := &tracesLatencyModule{}
	m.configure(db, getTenant)
	return m
}

type tracesLatencyModule struct{ handler *Handler }

func (m *tracesLatencyModule) Name() string                      { return "tracesLatency" }
func (m *tracesLatencyModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *tracesLatencyModule) configure(db clickhouse.Conn, getTenant registry.GetTenantFunc) {
	repo := NewRepository(db)
	svc := NewService(repo)
	m.handler = NewHandler(getTenant, svc)
}

func (m *tracesLatencyModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}

var _ modulecommon.GetTenantFunc = modulecommon.GetTenantFunc(nil)
