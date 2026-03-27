package detail

import (
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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/logs/surrounding", h.GetLogSurrounding)
	v1.GET("/logs/detail", h.GetLogDetail)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &logDetailModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type logDetailModule struct {
	handler *Handler
}

func (m *logDetailModule) Name() string                      { return "logDetail" }
func (m *logDetailModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *logDetailModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *logDetailModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
