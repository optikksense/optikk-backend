package producer

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
	shared.RegisterGET(v1, "/produce-rate-by-topic", h.GetProduceRateByTopic)
	shared.RegisterGET(v1, "/publish-latency-by-topic", h.GetPublishLatencyByTopic)
	shared.RegisterGET(v1, "/publish-errors", h.GetPublishErrors)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &producerModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type producerModule struct {
	handler *Handler
}

func (m *producerModule) Name() string { return "kafkaProducer" }

func (m *producerModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *producerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
