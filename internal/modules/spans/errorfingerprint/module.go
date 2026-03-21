package errorfingerprint

import (
	"github.com/gin-gonic/gin"
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

	v1.GET("/errors/fingerprints", h.ListFingerprints)
	v1.GET("/errors/fingerprints/trend", h.GetFingerprintTrend)
}

func init() {
	registry.Register(&errorFingerprintModule{})
}

type errorFingerprintModule struct {
	handler *Handler
}

func (m *errorFingerprintModule) Name() string                      { return "errorFingerprint" }
func (m *errorFingerprintModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *errorFingerprintModule) Init(deps registry.Deps) error {
	m.handler = NewHandler(
		deps.GetTenant,
		NewService(NewRepository(deps.NativeQuerier)),
	)
	return nil
}

func (m *errorFingerprintModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
