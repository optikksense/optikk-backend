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

// NewModule constructs the log search module.
func NewModule(deps *registry.Deps) (registry.Module, error) {
	svc := NewService(NewRepository(deps.NativeQuerier))
	return &logSearchModule{
		service: svc,
		handler: &Handler{
			DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
			Service:  svc,
		},
	}, nil
}

type logSearchModule struct {
	handler *Handler
	service *Service
}

func (m *logSearchModule) Name() string                      { return "logSearch" }
func (m *logSearchModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *logSearchModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
