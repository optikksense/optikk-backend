package spans

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/gin-gonic/gin"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

// Deps collects everything NewModule needs.
type Deps struct {
	Producer          *Producer
	CH                registry.ClickHouseConn
	PersistenceClient *kafkainfra.Consumer
}

// NewModule wires handler + persistence consumer.
func NewModule(d Deps) registry.Module {
	return &Module{
		handler:  NewHandler(d.Producer),
		consumer: NewConsumer(d.PersistenceClient, d.CH),
	}
}

type Module struct {
	handler  *Handler
	consumer *Consumer
}

func (m *Module) Name() string                      { return "spans" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	tracepb.RegisterTraceServiceServer(srv, m.handler)
}

func (m *Module) Start() {
	m.consumer.Start()
}

func (m *Module) Stop() error {
	_ = m.consumer.Stop() //nolint:errcheck
	return nil
}

var (
	_ registry.Module           = (*Module)(nil)
	_ registry.GRPCRegistrar    = (*Module)(nil)
	_ registry.BackgroundRunner = (*Module)(nil)
)
