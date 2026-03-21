package slowqueries

import (
	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/registry"
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
	g := v1.Group("/database/slow-queries")
	g.GET("/patterns", h.GetSlowQueryPatterns)
	g.GET("/collections", h.GetSlowestCollections)
	g.GET("/rate", h.GetSlowQueryRate)
	g.GET("/p99-by-text", h.GetP99ByQueryText)
}

func init() {
	registry.Register(&dbSlowModule{})
}

type dbSlowModule struct {
	handler *Handler
}

func (m *dbSlowModule) Name() string                      { return "dbSlow" }
func (m *dbSlowModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbSlowModule) Init(deps registry.Deps) error {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *dbSlowModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
