package errors

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
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
	v1.GET("/errors/groups", h.GetErrorGroups)
	v1.GET("/errors/groups/:groupId", h.GetErrorGroupDetail)
	v1.GET("/errors/groups/:groupId/traces", h.GetErrorGroupTraces)
	v1.GET("/errors/groups/:groupId/timeseries", h.GetErrorGroupTimeseries)
	v1.GET("/errors/groups/:groupId/latest-occurrence", h.GetErrorGroupLatestOccurrence)
	v1.GET("/errors/groups/:groupId/facets", h.GetErrorGroupFacets)

	v1.GET("/spans/error-hotspot", h.GetErrorHotspot)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc, redisClient *goredis.Client) registry.Module {
	module := &errorsModule{}
	module.configure(nativeQuerier, getTenant, redisClient)
	return module
}

type errorsModule struct {
	handler *ErrorHandler
}

func (m *errorsModule) Name() string { return "servicesErrors" }

func (m *errorsModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc, redisClient *goredis.Client) {
	m.handler = &ErrorHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *errorsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
