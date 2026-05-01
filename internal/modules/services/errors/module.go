package errors

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *ErrorHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/errors/service-error-rate", h.GetServiceErrorRate)
	v1.GET("/errors/error-volume", h.GetErrorVolume)
	v1.GET("/errors/latency-during-error-windows", h.GetLatencyDuringErrorWindows)
	v1.GET("/errors/groups", h.GetErrorGroups)
	v1.GET("/errors/groups/:groupId", h.GetErrorGroupDetail)
	v1.GET("/errors/groups/:groupId/traces", h.GetErrorGroupTraces)
	v1.GET("/errors/groups/:groupId/timeseries", h.GetErrorGroupTimeseries)

	v1.GET("/spans/exception-rate-by-type", h.GetExceptionRateByType)
	v1.GET("/spans/error-hotspot", h.GetErrorHotspot)
	v1.GET("/spans/http-5xx-by-route", h.GetHTTP5xxByRoute)

	v1.GET("/errors/fingerprints", h.ListFingerprints)
	v1.GET("/errors/fingerprints/trend", h.GetFingerprintTrend)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &errorsModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type errorsModule struct {
	handler *ErrorHandler
}

func (m *errorsModule) Name() string { return "servicesErrors" }

func (m *errorsModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &ErrorHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *errorsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
