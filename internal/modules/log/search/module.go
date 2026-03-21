package search

import (
	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/registry"
	sio "github.com/observability/observability-backend-go/internal/platform/socketio"
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
	v1.GET("/logs/stream", h.StreamLogs)
}

func init() {
	registry.Register(&logSearchModule{})
}

type logSearchModule struct {
	handler *Handler
	service *Service
}

func (m *logSearchModule) Name() string                      { return "logSearch" }
func (m *logSearchModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *logSearchModule) Init(deps registry.Deps) error {
	m.service = NewService(NewRepository(deps.NativeQuerier))
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  m.service,
	}
	return nil
}

func (m *logSearchModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}

func (m *logSearchModule) RegisterSocketIO(srv *sio.Server) {
	srv.RegisterHandler(SocketIOHandler(m.service))
}
