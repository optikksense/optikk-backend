package jvm

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func NewHandler(db clickhouse.Conn, getTenant modulecommon.GetTenantFunc, sketchQ *sketch.Querier) *JVMHandler {
	return &JVMHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(db), sketchQ),
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

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc, sketchQ *sketch.Querier) registry.Module {
	module := &jvmModule{}
	module.configure(nativeQuerier, getTenant, sketchQ)
	return module
}

type jvmModule struct {
	handler *JVMHandler
}

func (m *jvmModule) Name() string                      { return "jvm" }
func (m *jvmModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *jvmModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc, sketchQ *sketch.Querier) {
	m.handler = NewHandler(nativeQuerier, getTenant, sketchQ)
}

func (m *jvmModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
