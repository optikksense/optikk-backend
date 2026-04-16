package summary

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
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
	shared.RegisterDualGET(v1, "/summary", h.GetSummaryStats)
}

func NewModule(deps *registry.Deps) (registry.Module, error) {
	module := &dbSummaryModule{}
	module.configure(deps)
	return module, nil
}

type dbSummaryModule struct {
	handler *Handler
}

func (m *dbSummaryModule) Name() string                      { return "dbSummary" }
func (m *dbSummaryModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbSummaryModule) configure(deps *registry.Deps) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
}

func (m *dbSummaryModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
