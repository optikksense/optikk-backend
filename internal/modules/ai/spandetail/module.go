package spandetail

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// NewModule creates the AI span detail registry.Module.
func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	m := &aiSpanDetailModule{}
	m.configure(nativeQuerier, getTenant)
	return m
}

type aiSpanDetailModule struct {
	handler *Handler
}

func (m *aiSpanDetailModule) Name() string                      { return "aiSpanDetail" }
func (m *aiSpanDetailModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *aiSpanDetailModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *aiSpanDetailModule) RegisterRoutes(group *gin.RouterGroup) {
	ai := group.Group("/ai/spans")
	ai.GET("/:spanId", m.handler.GetSpanDetail)
	ai.GET("/:spanId/messages", m.handler.GetMessages)
	ai.GET("/:spanId/trace-context", m.handler.GetTraceContext)
	ai.GET("/:spanId/related", m.handler.GetRelatedSpans)
	ai.GET("/:spanId/token-breakdown", m.handler.GetTokenBreakdown)
}
