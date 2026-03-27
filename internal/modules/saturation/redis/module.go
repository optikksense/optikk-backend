package redis

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *RedisHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/saturation/redis/cache-hit-rate", h.GetCacheHitRate)
	v1.GET("/saturation/redis/replication-lag", h.GetReplicationLag)
	v1.GET("/saturation/redis/clients", h.GetClients)
	v1.GET("/saturation/redis/memory", h.GetMemory)
	v1.GET("/saturation/redis/memory-fragmentation", h.GetMemoryFragmentation)
	v1.GET("/saturation/redis/commands", h.GetCommands)
	v1.GET("/saturation/redis/evictions", h.GetEvictions)
	v1.GET("/saturation/redis/instances", h.GetInstances)
	v1.GET("/saturation/redis/keyspace", h.GetKeyspace)
	v1.GET("/saturation/redis/key-expiries", h.GetKeyExpiries)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &redisModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type redisModule struct {
	handler *RedisHandler
}

func (m *redisModule) Name() string                      { return "redis" }
func (m *redisModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *redisModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &RedisHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *redisModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
