// Package rules owns the rule lifecycle HTTP surface and the MySQL
// persistence for `observability.alerts` (rule row + inline instance state +
// inline silence list). Rule audit reads come from the ClickHouse event
// store owned by the engine subpackage.
package rules

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

func (m *Module) Name() string                      { return "alerting.rules" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *Module) RegisterRoutes(group *gin.RouterGroup) {
	a := group.Group("/alerts")
	a.POST("/rules", m.handler.Create)
	a.GET("/rules", m.handler.List)
	a.POST("/rules/preview", m.handler.Preview)
	a.GET("/rules/:id", m.handler.Get)
	a.PATCH("/rules/:id", m.handler.Update)
	a.DELETE("/rules/:id", m.handler.Delete)

	a.POST("/rules/:id/mute", m.handler.Mute)
	a.POST("/rules/:id/test", m.handler.Test)
	a.POST("/rules/:id/backtest", m.handler.Backtest)
	a.GET("/rules/:id/audit", m.handler.ListAudit)
}

var _ registry.Module = (*Module)(nil)
