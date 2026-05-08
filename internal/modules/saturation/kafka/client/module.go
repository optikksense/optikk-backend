package client

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
	shared.RegisterGET(v1, "/summary-stats", h.GetKafkaSummaryStats)
	shared.RegisterGET(v1, "/e2e-latency", h.GetE2ELatency)
	shared.RegisterGET(v1, "/broker-connections", h.GetBrokerConnections)
	shared.RegisterGET(v1, "/client-op-duration", h.GetClientOperationDuration)
	shared.RegisterGET(v1, "/client-op-errors", h.GetClientOpErrors)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &clientModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type clientModule struct {
	handler *Handler
}

func (m *clientModule) Name() string { return "kafkaClient" }

func (m *clientModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *clientModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
