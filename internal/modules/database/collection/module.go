package collection

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
	g := v1.Group("/database/collection")
	g.GET("/latency", h.GetCollectionLatency)
	g.GET("/ops", h.GetCollectionOps)
	g.GET("/errors", h.GetCollectionErrors)
	g.GET("/query-texts", h.GetCollectionQueryTexts)
	g.GET("/read-vs-write", h.GetCollectionReadVsWrite)
}

func init() {
	registry.Register(&dbCollectionModule{})
}

type dbCollectionModule struct {
	handler *Handler
}

func (m *dbCollectionModule) Name() string                      { return "dbCollection" }
func (m *dbCollectionModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbCollectionModule) Init(deps registry.Deps) error {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *dbCollectionModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
