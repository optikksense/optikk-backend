package saturation

import "github.com/gin-gonic/gin"

// Config holds saturation-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default saturation-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts saturation routes.
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *SaturationHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/saturation/kafka/queue-lag", h.GetKafkaQueueLag)
	v1.GET("/saturation/kafka/production-rate", h.GetKafkaProductionRate)
	v1.GET("/saturation/kafka/consumption-rate", h.GetKafkaConsumptionRate)
	v1.GET("/saturation/database/query-by-table", h.GetDatabaseQueryByTable)
	v1.GET("/saturation/database/avg-latency", h.GetDatabaseAvgLatency)

	v1.GET("/saturation/database/latency-summary", h.GetDatabaseCacheSummary)
	v1.GET("/saturation/database/systems", h.GetDatabaseSystems)
	v1.GET("/saturation/database/top-tables", h.GetDatabaseTopTables)

	v1.GET("/saturation/queue/consumer-lag", h.GetQueueConsumerLag)
	v1.GET("/saturation/queue/topic-lag", h.GetQueueTopicLag)
	v1.GET("/saturation/queue/top-queues", h.GetQueueTopQueues)
}
