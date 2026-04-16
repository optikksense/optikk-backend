package httpmetrics

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *HTTPMetricsHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/http")
	g.GET("/request-rate", h.GetRequestRate)
	g.GET("/request-duration", h.GetRequestDuration)
	g.GET("/active-requests", h.GetActiveRequests)
	g.GET("/request-body-size", h.GetRequestBodySize)
	g.GET("/response-body-size", h.GetResponseBodySize)
	g.GET("/client-duration", h.GetClientDuration)
	g.GET("/dns-duration", h.GetDNSDuration)
	g.GET("/tls-duration", h.GetTLSDuration)
	g.GET("/status-distribution", h.GetStatusDistribution)
	g.GET("/error-timeseries", h.GetErrorTimeseries)

	routes := g.Group("/routes")
	routes.GET("/top-by-volume", h.GetTopRoutesByVolume)
	routes.GET("/top-by-latency", h.GetTopRoutesByLatency)
	routes.GET("/error-rate", h.GetRouteErrorRate)
	routes.GET("/error-timeseries", h.GetRouteErrorTimeseries)

	external := g.Group("/external")
	external.GET("/top-hosts", h.GetTopExternalHosts)
	external.GET("/host-latency", h.GetExternalHostLatency)
	external.GET("/error-rate", h.GetExternalHostErrorRate)
}

func NewModule(deps *registry.Deps) (registry.Module, error) {
	module := &httpMetricsModule{}
	module.configure(deps)
	return module, nil
}

type httpMetricsModule struct {
	handler *HTTPMetricsHandler
}

func (m *httpMetricsModule) Name() string                      { return "httpMetrics" }
func (m *httpMetricsModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *httpMetricsModule) configure(deps *registry.Deps) {
	m.handler = &HTTPMetricsHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
}

func (m *httpMetricsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
