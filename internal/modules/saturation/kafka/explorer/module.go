package explorer

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	saturationkafka "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/saturation/kafka/summary", h.GetKafkaSummary)
	v1.GET("/saturation/kafka/topics", h.GetKafkaTopics)
	v1.GET("/saturation/kafka/groups", h.GetKafkaGroups)
	v1.GET("/saturation/kafka/topic/overview", h.GetKafkaTopicOverview)
	v1.GET("/saturation/kafka/topic/groups", h.GetKafkaTopicGroups)
	v1.GET("/saturation/kafka/topic/partitions", h.GetKafkaTopicPartitions)
	v1.GET("/saturation/kafka/group/overview", h.GetKafkaGroupOverview)
	v1.GET("/saturation/kafka/group/topics", h.GetKafkaGroupTopics)
	v1.GET("/saturation/kafka/group/partitions", h.GetKafkaGroupPartitions)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &kafkaExplorerModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type kafkaExplorerModule struct {
	handler *Handler
}

func (m *kafkaExplorerModule) Name() string { return "saturationKafkaExplorer" }

func (m *kafkaExplorerModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service: NewService(
			saturationkafka.NewService(saturationkafka.NewRepository(nativeQuerier)),
		),
	}
}

func (m *kafkaExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
