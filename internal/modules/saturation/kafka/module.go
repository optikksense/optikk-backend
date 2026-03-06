package kafka

import "github.com/gin-gonic/gin"

// Config holds kafka saturation route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts saturation routes for the kafka module.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *KafkaHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/saturation/kafka/queue-lag", h.GetKafkaQueueLag)
	v1.GET("/saturation/kafka/production-rate", h.GetKafkaProductionRate)
	v1.GET("/saturation/kafka/consumption-rate", h.GetKafkaConsumptionRate)

	v1.GET("/saturation/queue/consumer-lag", h.GetQueueConsumerLag)
	v1.GET("/saturation/queue/topic-lag", h.GetQueueTopicLag)
	v1.GET("/saturation/queue/top-queues", h.GetQueueTopQueues)

	// OTel messaging.* standard metrics
	v1.GET("/saturation/queue/consumer-lag-detail", h.GetConsumerLagPerPartition)
	v1.GET("/saturation/queue/message-rates", h.GetMessageRates)
	v1.GET("/saturation/queue/operation-duration", h.GetOperationDuration)
	v1.GET("/saturation/queue/offset-commit-rate", h.GetOffsetCommitRate)
}
