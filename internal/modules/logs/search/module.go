package search

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
	v1.GET("/logs", h.GetLogs)
}

// NewModule constructs the log search module. Pass a non-nil service to share the instance with live tail WebSocket.
func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc, svc *Service) registry.Module {
	module := &logSearchModule{}
	module.configure(nativeQuerier, getTenant, svc)
	return module
}

type logSearchModule struct {
	handler *Handler
	service *Service
}

func (m *logSearchModule) Name() string                      { return "logSearch" }
func (m *logSearchModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *logSearchModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc, svc *Service) {
	if svc != nil {
		m.service = svc
	} else {
		m.service = NewService(NewRepository(nativeQuerier))
	}
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  m.service,
	}
}

func (m *logSearchModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
