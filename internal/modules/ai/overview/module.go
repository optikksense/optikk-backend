package overview

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// NewModule creates the AI overview registry.Module.
func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	m := &aiOverviewModule{}
	m.configure(nativeQuerier, getTenant)
	return m
}

type aiOverviewModule struct {
	handler *Handler
}

func (m *aiOverviewModule) Name() string                      { return "aiOverview" }
func (m *aiOverviewModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *aiOverviewModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *aiOverviewModule) RegisterRoutes(group *gin.RouterGroup) {
	ai := group.Group("/ai/overview")
	ai.GET("/summary", m.handler.GetSummary)
	ai.GET("/models", m.handler.GetModels)
	ai.GET("/operations", m.handler.GetOperations)
	ai.GET("/services", m.handler.GetServices)
	ai.GET("/model-health", m.handler.GetModelHealth)
	ai.GET("/top-slow", m.handler.GetTopSlow)
	ai.GET("/top-errors", m.handler.GetTopErrors)
	ai.GET("/finish-reasons", m.handler.GetFinishReasons)
	ai.GET("/timeseries/requests", m.handler.GetTimeseriesRequests)
	ai.GET("/timeseries/latency", m.handler.GetTimeseriesLatency)
	ai.GET("/timeseries/errors", m.handler.GetTimeseriesErrors)
	ai.GET("/timeseries/tokens", m.handler.GetTimeseriesTokens)
	ai.GET("/timeseries/throughput", m.handler.GetTimeseriesThroughput)
	ai.GET("/timeseries/cost", m.handler.GetTimeseriesCost)
}
