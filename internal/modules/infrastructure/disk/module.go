package disk

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

func NewHandler(db *dbutil.NativeQuerier, getTenant modulecommon.GetTenantFunc) *DiskHandler {
	return &DiskHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(db)),
	}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *DiskHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/infrastructure/disk")
	g.GET("/io", h.GetDiskIO)
	g.GET("/operations", h.GetDiskOperations)
	g.GET("/io-time", h.GetDiskIOTime)
	g.GET("/filesystem-usage", h.GetFilesystemUsage)
	g.GET("/filesystem-utilization", h.GetFilesystemUtilization)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &diskModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type diskModule struct {
	handler *DiskHandler
}

func (m *diskModule) Name() string                      { return "disk" }
func (m *diskModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *diskModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = NewHandler(nativeQuerier, getTenant)
}

func (m *diskModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
