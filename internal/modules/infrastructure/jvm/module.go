package jvm

import (
	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/database"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/registry"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func NewHandler(db *database.NativeQuerier, getTenant modulecommon.GetTenantFunc) *JVMHandler {
	return &JVMHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(db)),
	}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *JVMHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/infrastructure/jvm")
	g.GET("/memory", h.GetJVMMemory)
	g.GET("/gc-duration", h.GetJVMGCDuration)
	g.GET("/gc-collections", h.GetJVMGCCollections)
	g.GET("/threads", h.GetJVMThreadCount)
	g.GET("/classes", h.GetJVMClasses)
	g.GET("/cpu", h.GetJVMCPU)
	g.GET("/buffers", h.GetJVMBuffers)
}

func init() {
	registry.Register(&jvmModule{})
}

type jvmModule struct {
	handler *JVMHandler
}

func (m *jvmModule) Name() string                      { return "jvm" }
func (m *jvmModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *jvmModule) Init(deps registry.Deps) error {
	m.handler = NewHandler(deps.NativeQuerier, deps.GetTenant)
	return nil
}

func (m *jvmModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
