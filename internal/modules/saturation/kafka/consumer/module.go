package consumer

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka/internal/shared"
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
	shared.RegisterGET(v1, "/consume-rate-by-topic", h.GetConsumeRateByTopic)
	shared.RegisterGET(v1, "/receive-latency-by-topic", h.GetReceiveLatencyByTopic)
	shared.RegisterGET(v1, "/consume-rate-by-group", h.GetConsumeRateByGroup)
	shared.RegisterGET(v1, "/process-rate-by-group", h.GetProcessRateByGroup)
	shared.RegisterGET(v1, "/process-latency-by-group", h.GetProcessLatencyByGroup)
	shared.RegisterGET(v1, "/consume-errors", h.GetConsumeErrors)
	shared.RegisterGET(v1, "/process-errors", h.GetProcessErrors)
	shared.RegisterGET(v1, "/consumer-lag-by-group", h.GetConsumerLagByGroup)
	shared.RegisterGET(v1, "/lag-per-partition", h.GetConsumerLagPerPartition)
	shared.RegisterGET(v1, "/rebalance-signals", h.GetRebalanceSignals)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &consumerModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type consumerModule struct {
	handler *Handler
}

func (m *consumerModule) Name() string { return "kafkaConsumer" }

func (m *consumerModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *consumerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
