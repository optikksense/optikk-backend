package livetail

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	sio "github.com/Optikk-Org/optikk-backend/internal/infra/socketio"
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

	v1.GET("/spans/live-tail", h.GetLiveTail)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &liveTailModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type liveTailModule struct {
	handler *Handler
	service *Service
}

func (m *liveTailModule) Name() string                      { return "liveTail" }
func (m *liveTailModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *liveTailModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.service = NewService(NewRepository(nativeQuerier))
	m.handler = NewHandler(getTenant, m.service)
}

func (m *liveTailModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}

func (m *liveTailModule) RegisterSocketIO(srv *sio.Server) {
	srv.RegisterHandler(SocketIOHandler(m.service))
}
