package logs

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/gin-gonic/gin"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
)

// Deps collects everything NewModule needs.
type Deps struct {
	Producer          *Producer
	CH                registry.ClickHouseConn
	PersistenceClient *kafkainfra.Consumer
	KafkaBase         *kafkainfra.Producer
	TopicPrefix       string
}

// NewModule wires the handler + dispatcher (generic PollFetches + per-partition
// worker + retrying CH writer + DLQ). The legacy single-goroutine Consumer
// stays compiled for rollback but is no longer on the hot path.
func NewModule(d Deps) registry.Module {
	dlq := NewDLQProducer(d.KafkaBase, d.TopicPrefix)
	return &Module{
		handler:    NewHandler(d.Producer),
		dispatcher: NewDispatcher(d.PersistenceClient, d.CH, dlq),
	}
}

type Module struct {
	handler    *Handler
	dispatcher *Dispatcher
}

func (m *Module) Name() string                      { return "logs" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	logspb.RegisterLogsServiceServer(srv, m.handler)
}

func (m *Module) Start() {
	m.dispatcher.Start()
}

func (m *Module) Stop() error {
	return m.dispatcher.Stop()
}

var (
	_ registry.Module           = (*Module)(nil)
	_ registry.GRPCRegistrar    = (*Module)(nil)
	_ registry.BackgroundRunner = (*Module)(nil)
)
