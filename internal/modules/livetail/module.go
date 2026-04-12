package livetail

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

// NewModule registers GET /api/v1/ws/live (WebSocket live tail).
func NewModule(cfg Config) registry.Module {
	return &liveTailModule{handler: NewHandler(cfg)}
}

type liveTailModule struct {
	handler gin.HandlerFunc
}

func (m *liveTailModule) Name() string                      { return "livetail" }
func (m *liveTailModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *liveTailModule) RegisterRoutes(group *gin.RouterGroup) {
	group.GET("/ws/live", m.handler)
}
