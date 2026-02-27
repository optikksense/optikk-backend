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

	v1.GET("/saturation/database-cache", h.GetInsightDatabaseCache)
	v1.GET("/saturation/messaging-queue", h.GetInsightMessagingQueue)
}
