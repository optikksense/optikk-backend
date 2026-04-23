package errors

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
	v1.GET("/errors/groups", h.ErrorGroups)
	v1.GET("/errors/timeseries", h.Timeseries)
	v1.GET("/services/:serviceName/errors", h.ServiceErrorGroups)
	v1.GET("/services/:serviceName/errors/timeseries", h.ServiceTimeseries)
}

func NewModule(db clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	m := &tracesErrorsModule{}
	m.configure(db, getTenant)
	return m
}

type tracesErrorsModule struct{ handler *Handler }

func (m *tracesErrorsModule) Name() string                      { return "tracesErrors" }
func (m *tracesErrorsModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *tracesErrorsModule) configure(db clickhouse.Conn, getTenant registry.GetTenantFunc) {
	repo := NewRepository(db)
	svc := NewService(repo)
	m.handler = NewHandler(getTenant, svc)
}

func (m *tracesErrorsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}

var _ modulecommon.GetTenantFunc = modulecommon.GetTenantFunc(nil)
