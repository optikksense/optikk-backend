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

	v1.GET("/overview/errors/service-error-rate", h.GetServiceErrorRate)
	v1.GET("/overview/errors/error-volume", h.GetErrorVolume)
	v1.GET("/overview/errors/latency-during-error-windows", h.GetLatencyDuringErrorWindows)
	v1.GET("/overview/errors/groups", h.GetErrorGroups)
	v1.GET("/errors/groups/:groupId", h.GetErrorGroupDetail)
	v1.GET("/errors/groups/:groupId/traces", h.GetErrorGroupTraces)
	v1.GET("/errors/groups/:groupId/timeseries", h.GetErrorGroupTimeseries)

	// Migrated from errortracking
	v1.GET("/spans/exception-rate-by-type", h.GetExceptionRateByType)
	v1.GET("/spans/error-hotspot", h.GetErrorHotspot)
	v1.GET("/spans/http-5xx-by-route", h.GetHTTP5xxByRoute)

	// Migrated from errorfingerprint
	v1.GET("/errors/fingerprints", h.ListFingerprints)
	v1.GET("/errors/fingerprints/trend", h.GetFingerprintTrend)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &overviewErrorsModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type overviewErrorsModule struct {
	handler *ErrorHandler
}

func (m *overviewErrorsModule) Name() string { return "overviewErrors" }

func (m *overviewErrorsModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &ErrorHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *overviewErrorsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
