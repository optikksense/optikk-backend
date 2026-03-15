package redis

import "github.com/gin-gonic/gin"

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
	v1.GET("/saturation/redis/keyspace", h.GetKeyspace)
	v1.GET("/saturation/redis/key-expiries", h.GetKeyExpiries)
}
