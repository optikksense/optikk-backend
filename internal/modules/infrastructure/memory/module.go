package memory

import (
	"github.com/gin-gonic/gin"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func NewHandler(db dbutil.Querier, getTenant modulecommon.GetTenantFunc) *MemoryHandler {
	return &MemoryHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(db)),
	}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *MemoryHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/infrastructure/memory")
	g.GET("/usage", h.GetMemoryUsage)
	g.GET("/usage-percentage", h.GetMemoryUsagePercentage)
	g.GET("/swap", h.GetSwapUsage)
}
