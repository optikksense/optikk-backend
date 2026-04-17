// Package silences hosts /alerts/silences/* — silence CRUD. Silence rows
// are stored inline on the owning rule, so the service delegates to
// rules.Repository for persistence.
package silences

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

func (m *Module) Name() string                      { return "alerting.silences" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *Module) RegisterRoutes(group *gin.RouterGroup) {
	a := group.Group("/alerts")
	a.GET("/silences", m.handler.List)
	a.POST("/silences", m.handler.Create)
	a.PATCH("/silences/:id", m.handler.Update)
	a.DELETE("/silences/:id", m.handler.Delete)
}

var _ registry.Module = (*Module)(nil)
