package database

import "github.com/gin-gonic/gin"

// Config holds database saturation route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts saturation routes for the database module.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *DatabaseHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/saturation/database/query-by-table", h.GetDatabaseQueryByTable)
	v1.GET("/saturation/database/avg-latency", h.GetDatabaseAvgLatency)
	v1.GET("/saturation/database/latency-summary", h.GetDatabaseCacheSummary)
	v1.GET("/saturation/database/systems", h.GetDatabaseSystems)
	v1.GET("/saturation/database/top-tables", h.GetDatabaseTopTables)

	// db.client.* OTel standard metrics
	v1.GET("/saturation/database/connection-count", h.GetConnectionCount)
	v1.GET("/saturation/database/connection-wait-time", h.GetConnectionWaitTime)
	v1.GET("/saturation/database/connection-pending", h.GetConnectionPending)
	v1.GET("/saturation/database/connection-timeouts", h.GetConnectionTimeouts)
	v1.GET("/saturation/database/query-duration", h.GetQueryDuration)

	// Redis metrics
	v1.GET("/saturation/redis/cache-hit-rate", h.GetRedisCacheHitRate)
	v1.GET("/saturation/redis/clients", h.GetRedisConnectedClients)
	v1.GET("/saturation/redis/memory", h.GetRedisMemoryUsed)
	v1.GET("/saturation/redis/memory-fragmentation", h.GetRedisMemoryFragmentation)
	v1.GET("/saturation/redis/commands", h.GetRedisCommandRate)
	v1.GET("/saturation/redis/evictions", h.GetRedisEvictions)
	v1.GET("/saturation/redis/keyspace", h.GetRedisKeyspaceSize)
	v1.GET("/saturation/redis/key-expiries", h.GetRedisKeyExpiries)
	v1.GET("/saturation/redis/replication-lag", h.GetRedisReplicationLag)
}
