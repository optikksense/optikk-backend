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

// RegisterRoutes is a no-op: live tail is exposed only via Socket.IO (see RegisterSocketIO).
func RegisterRoutes(_ Config, _ *gin.RouterGroup) {}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &liveTailModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type liveTailModule struct {
	service *Service
}

func (m *liveTailModule) Name() string                      { return "liveTail" }
func (m *liveTailModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *liveTailModule) configure(nativeQuerier *registry.NativeQuerier, _ registry.GetTenantFunc) {
	m.service = NewService(NewRepository(nativeQuerier))
}

func (m *liveTailModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group)
}

func (m *liveTailModule) RegisterSocketIO(srv *sio.Server) {
	srv.RegisterHandler(SocketIOHandler(m.service))
}
