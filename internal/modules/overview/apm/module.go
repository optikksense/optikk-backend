package apm

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *APMHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/apm")
	g.GET("/rpc-duration", h.GetRPCDuration)
	g.GET("/rpc-request-rate", h.GetRPCRequestRate)
	g.GET("/messaging-publish-duration", h.GetMessagingPublishDuration)
	g.GET("/process-cpu", h.GetProcessCPU)
	g.GET("/process-memory", h.GetProcessMemory)
	g.GET("/open-fds", h.GetOpenFDs)
	g.GET("/uptime", h.GetUptime)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &apmModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type apmModule struct {
	handler *APMHandler
}

func (m *apmModule) Name() string                      { return "apm" }
func (m *apmModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *apmModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &APMHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *apmModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
