package network

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

func NewHandler(db *database.NativeQuerier, getTenant modulecommon.GetTenantFunc) *NetworkHandler {
	return &NetworkHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(db)),
	}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *NetworkHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/infrastructure/network")
	g.GET("/io", h.GetNetworkIO)
	g.GET("/packets", h.GetNetworkPackets)
	g.GET("/errors", h.GetNetworkErrors)
	g.GET("/dropped", h.GetNetworkDropped)
	g.GET("/connections", h.GetNetworkConnections)
}
