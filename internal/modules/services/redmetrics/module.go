package redmetrics

import (
	"github.com/ClickHouse/clickhouse-go/v2"
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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *REDMetricsHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	red := v1.Group("/spans/red")
	red.GET("/summary", h.GetSummary)
	red.GET("/apdex", h.GetApdex)
	red.GET("/request-and-error-rate", h.GetRequestAndErrorRateTimeSeries)
	red.GET("/status-timeseries", h.GetStatusTimeSeries)
	red.GET("/latency-percentiles-timeseries", h.GetLatencyPercentilesTimeSeries)
	red.GET("/top-endpoints", h.GetTopEndpointsCombined)
	red.GET("/services/:serviceName/summary", h.GetServiceSummary)
	red.GET("/operation-baseline", h.GetOperationBaseline)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &redMetricsModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type redMetricsModule struct {
	handler *REDMetricsHandler
}

func (m *redMetricsModule) Name() string { return "redMetrics" }

func (m *redMetricsModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &REDMetricsHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *redMetricsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
