package notifications

import (
	"github.com/gin-gonic/gin"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/dispatch"
)

// Module wires channels / policies / templates / integrations endpoints.
type Module struct {
	handler *Handler
}

func NewModule(sqlDB *registry.SQLDB, getTenant registry.GetTenantFunc) *Module {
	repo := NewRepository(sqlDB)
	dispatcher := dispatch.NewDefaultDispatcher()
	svc := NewService(repo, dispatcher)
	return &Module{handler: NewHandler(getTenant, svc)}
}

func (m *Module) Name() string { return "alerting.notifications" }

func (m *Module) RegisterRoutes(v1 *gin.RouterGroup) {
	g := v1.Group("/notifications")

	ch := g.Group("/channels")
	ch.GET("", m.handler.ListChannels)
	ch.POST("", m.handler.CreateChannel)
	ch.GET("/:id", m.handler.GetChannel)
	ch.PUT("/:id", m.handler.UpdateChannel)
	ch.DELETE("/:id", m.handler.DeleteChannel)
	ch.POST("/:id/test", m.handler.TestChannel)

	g.GET("/integrations", m.handler.ListIntegrations)

	pol := g.Group("/policies")
	pol.GET("", m.handler.ListPolicies)
	pol.POST("", m.handler.CreatePolicy)
	pol.PUT("/:id", m.handler.UpdatePolicy)
	pol.DELETE("/:id", m.handler.DeletePolicy)

	tpl := g.Group("/templates")
	tpl.GET("", m.handler.ListTemplates)
	tpl.POST("", m.handler.CreateTemplate)
	tpl.PUT("/:id", m.handler.UpdateTemplate)
	tpl.DELETE("/:id", m.handler.DeleteTemplate)
}
