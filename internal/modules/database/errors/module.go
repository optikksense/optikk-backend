package errors

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
	g := v1.Group("/database/errors")
	g.GET("/by-system", h.GetErrorsBySystem)
	g.GET("/by-operation", h.GetErrorsByOperation)
	g.GET("/by-error-type", h.GetErrorsByErrorType)
	g.GET("/by-collection", h.GetErrorsByCollection)
	g.GET("/by-status", h.GetErrorsByResponseStatus)
	g.GET("/ratio", h.GetErrorRatio)
}

func init() {
	registry.Register(&dbErrorsModule{})
}

type dbErrorsModule struct {
	handler *Handler
}

func (m *dbErrorsModule) Name() string                      { return "dbErrors" }
func (m *dbErrorsModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbErrorsModule) Init(deps registry.Deps) error {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *dbErrorsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
