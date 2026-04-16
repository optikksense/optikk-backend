package slowqueries

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
	shared.RegisterDualGroup(v1, "/slow-queries", func(g *gin.RouterGroup) {
		g.GET("/patterns", h.GetSlowQueryPatterns)
		g.GET("/collections", h.GetSlowestCollections)
		g.GET("/rate", h.GetSlowQueryRate)
		g.GET("/p99-by-text", h.GetP99ByQueryText)
	})
}

func NewModule(deps *registry.Deps) (registry.Module, error) {
	return &dbSlowModule{
		handler: &Handler{
			DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
			Service:  NewService(NewRepository(deps.NativeQuerier)),
		},
	}, nil
}

type dbSlowModule struct {
	handler *Handler
}

func (m *dbSlowModule) Name() string                      { return "dbSlow" }
func (m *dbSlowModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbSlowModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
