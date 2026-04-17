// Package incidents hosts the incident HTTP surface — /alerts/incidents
// (firing-instance list), /alerts/activity (recent event feed), and the
// per-instance state changes (/alerts/instances/:id/{ack,snooze}).
package incidents

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

type Module struct {
	handler *Handler
}

func NewModule(handler *Handler) registry.Module {
	return &Module{handler: handler}
}

func (m *Module) Name() string                      { return "alerting.incidents" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *Module) RegisterRoutes(group *gin.RouterGroup) {
	a := group.Group("/alerts")
	a.GET("/incidents", m.handler.List)
	a.GET("/activity", m.handler.ListActivity)
	a.POST("/instances/:id/ack", m.handler.Ack)
	a.POST("/instances/:id/snooze", m.handler.Snooze)
}

var _ registry.Module = (*Module)(nil)
