package disk

import (
	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/database"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func NewHandler(db *database.NativeQuerier, getTenant modulecommon.GetTenantFunc) *DiskHandler {
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
