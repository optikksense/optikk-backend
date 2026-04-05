package livetail

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes is a no-op: live tail is exposed via WebSocket (see server wiring).
func RegisterRoutes(_ Config, _ *gin.RouterGroup) {}

// NewModule constructs the traces live tail module. Pass a non-nil service to share the instance with live tail WebSocket.
func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc, svc *Service) registry.Module {
	module := &liveTailModule{}
	module.configure(nativeQuerier, getTenant, svc)
	return module
}

type liveTailModule struct {
	service *Service
}

func (m *liveTailModule) Name() string                      { return "liveTail" }
func (m *liveTailModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *liveTailModule) configure(nativeQuerier *registry.NativeQuerier, _ registry.GetTenantFunc, svc *Service) {
	if svc != nil {
		m.service = svc
	} else {
		m.service = NewService(NewRepository(nativeQuerier))
	}
}

func (m *liveTailModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group)
}
