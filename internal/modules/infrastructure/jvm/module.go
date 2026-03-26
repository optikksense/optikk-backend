package jvm

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func NewHandler(db *dbutil.NativeQuerier, getTenant modulecommon.GetTenantFunc) *JVMHandler {
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

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &jvmModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type jvmModule struct {
	handler *JVMHandler
}

func (m *jvmModule) Name() string                      { return "jvm" }
func (m *jvmModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *jvmModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = NewHandler(nativeQuerier, getTenant)
}

func (m *jvmModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
