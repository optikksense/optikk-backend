package volume

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
	g := v1.Group("/database/ops")
	g.GET("/by-system", h.GetOpsBySystem)
	g.GET("/by-operation", h.GetOpsByOperation)
	g.GET("/by-collection", h.GetOpsByCollection)
	g.GET("/read-vs-write", h.GetReadVsWrite)
	g.GET("/by-namespace", h.GetOpsByNamespace)
}

func init() {
	registry.Register(&dbVolumeModule{})
}

type dbVolumeModule struct {
	handler *Handler
}

func (m *dbVolumeModule) Name() string                      { return "dbVolume" }
func (m *dbVolumeModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbVolumeModule) Init(deps registry.Deps) error {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *dbVolumeModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
