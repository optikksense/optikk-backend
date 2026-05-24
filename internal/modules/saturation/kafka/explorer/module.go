package explorer

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/saturation/kafka/topics/throughput", h.GetTopicThroughput)
	v1.GET("/saturation/kafka/topics/lag", h.GetTopicLag)
	v1.GET("/saturation/kafka/topics/consumers", h.GetTopicConsumers)

	v1.GET("/saturation/kafka/groups/partitions", h.GetGroupPartitions)
	v1.GET("/saturation/kafka/groups/commits", h.GetGroupCommits)
	v1.GET("/saturation/kafka/groups/fetches", h.GetGroupFetches)
	v1.GET("/saturation/kafka/groups/health", h.GetGroupHealth)

	v1.GET("/saturation/kafka/topic/groups/throughput", h.GetTopicGroupThroughput)
	v1.GET("/saturation/kafka/topic/groups/lag", h.GetTopicGroupLag)

	v1.GET("/saturation/kafka/group/topics", h.GetGroupTopics)
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
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *kafkaExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
