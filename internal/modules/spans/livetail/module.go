package livetail

import (
	"github.com/gin-gonic/gin"
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

	v1.GET("/spans/live-tail", h.GetLiveTail)
}

func init() {
	registry.Register(&liveTailModule{})
}

type liveTailModule struct {
	handler *Handler
	service *Service
}

func (m *liveTailModule) Name() string                      { return "liveTail" }
func (m *liveTailModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *liveTailModule) Init(deps registry.Deps) error {
	m.service = NewService(NewRepository(deps.NativeQuerier))
	m.handler = NewHandler(deps.GetTenant, m.service)
	return nil
}

func (m *liveTailModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}

func (m *liveTailModule) RegisterSocketIO(srv *sio.Server) {
	srv.RegisterHandler(SocketIOHandler(m.service))
}
