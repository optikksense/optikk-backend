package savedviews

import (
	"github.com/gin-gonic/gin"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
)

type module struct {
	handler *Handler
}

func NewModule(sqlDB *registry.SQLDB, getTenant registry.GetTenantFunc) registry.Module {
	m := &module{}
	m.handler = NewHandler(getTenant, NewService(NewRepository(sqlDB.DB)))
	return m
}

func (m *module) Name() string { return "savedViews" }

func (m *module) RegisterRoutes(group *gin.RouterGroup) {
	if m.handler == nil {
		return
	}
	g := group.Group("/saved-views")
	g.GET("", m.handler.List)
	g.POST("", m.handler.Create)
	g.DELETE("/:id", m.handler.Delete)
}
