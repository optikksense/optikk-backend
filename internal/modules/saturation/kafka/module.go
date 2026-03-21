package kafka

import (
	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/registry"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *KafkaHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	// Summary
	v1.GET("/saturation/kafka/summary-stats", h.GetKafkaSummaryStats)

	// Dashboard 1: Production Rate
	v1.GET("/saturation/kafka/produce-rate-by-topic", h.GetProduceRateByTopic)
	v1.GET("/saturation/kafka/publish-latency-by-topic", h.GetPublishLatencyByTopic)

	// Dashboard 2: Consumption Rate — by Topic
	v1.GET("/saturation/kafka/consume-rate-by-topic", h.GetConsumeRateByTopic)
	v1.GET("/saturation/kafka/receive-latency-by-topic", h.GetReceiveLatencyByTopic)

	// Dashboard 3: Consumption Rate — by Consumer Group
	v1.GET("/saturation/kafka/consume-rate-by-group", h.GetConsumeRateByGroup)
	v1.GET("/saturation/kafka/process-rate-by-group", h.GetProcessRateByGroup)
	v1.GET("/saturation/kafka/process-latency-by-group", h.GetProcessLatencyByGroup)

	// Dashboard 4: Consumer Lag
	v1.GET("/saturation/kafka/consumer-lag-by-group", h.GetConsumerLagByGroup)
	v1.GET("/saturation/kafka/lag-by-group", h.GetConsumerLagByGroup)
	v1.GET("/saturation/kafka/lag-per-partition", h.GetConsumerLagPerPartition)

	// Dashboard 5: Consumer Group Rebalancing
	v1.GET("/saturation/kafka/assigned-partitions", h.GetRebalanceSignals)
	v1.GET("/saturation/kafka/rebalance-signals", h.GetRebalanceSignals)

	// Dashboard 6: End-to-End Latency
	v1.GET("/saturation/kafka/e2e-latency", h.GetE2ELatency)

	// Dashboard 7: Error Rates
	v1.GET("/saturation/kafka/publish-errors", h.GetPublishErrors)
	v1.GET("/saturation/kafka/consume-errors", h.GetConsumeErrors)
	v1.GET("/saturation/kafka/process-errors", h.GetProcessErrors)
	v1.GET("/saturation/kafka/client-op-errors", h.GetClientOpErrors)

	// Dashboard 8: Broker / Client Connectivity
	v1.GET("/saturation/kafka/broker-connections", h.GetBrokerConnections)
	v1.GET("/saturation/kafka/client-op-duration", h.GetClientOperationDuration)
}

func init() {
	registry.Register(&kafkaModule{})
}

type kafkaModule struct {
	handler *KafkaHandler
}

func (m *kafkaModule) Name() string                      { return "kafka" }
func (m *kafkaModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *kafkaModule) Init(deps registry.Deps) error {
	m.handler = &KafkaHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *kafkaModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
