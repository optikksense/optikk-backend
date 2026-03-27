package errorfingerprint

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/errors/fingerprints", h.ListFingerprints)
	v1.GET("/errors/fingerprints/trend", h.GetFingerprintTrend)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &errorFingerprintModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type errorFingerprintModule struct {
	handler *Handler
}

func (m *errorFingerprintModule) Name() string                      { return "errorFingerprint" }
func (m *errorFingerprintModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *errorFingerprintModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = NewHandler(
		getTenant,
		NewService(NewRepository(nativeQuerier)),
	)
}

func (m *errorFingerprintModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
