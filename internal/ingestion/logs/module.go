package logs

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/modules/livetail"
	"github.com/gin-gonic/gin"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
)

// Deps collects everything NewModule needs. Keeping a struct here keeps the
// call-site in modules_manifest.go readable as the deps list grows.
type Deps struct {
	Producer          *Producer
	CH                registry.ClickHouseConn
	PersistenceClient *kafkainfra.Consumer
	LivetailClient    *kafkainfra.Consumer
	Hub               livetail.Hub
}

// NewModule wires the handler, persistence consumer, and livetail consumer
// into a single registry.Module so the gRPC server + background runners both
// get registered from one call in modules_manifest.go.
func NewModule(d Deps) registry.Module {
	return &Module{
		handler:  NewHandler(d.Producer),
		consumer: NewConsumer(d.PersistenceClient, d.CH),
		livetail: NewLivetail(d.LivetailClient, d.Hub),
	}
}

type Module struct {
	handler  *Handler
	consumer *Consumer
	livetail *Livetail
}

func (m *Module) Name() string                      { return "logs" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	logspb.RegisterLogsServiceServer(srv, m.handler)
}

func (m *Module) Start() {
	m.consumer.Start()
	m.livetail.Start()
}

func (m *Module) Stop() error {
	_ = m.consumer.Stop() //nolint:errcheck // each returns nil
	_ = m.livetail.Stop() //nolint:errcheck
	return nil
}

var (
	_ registry.Module           = (*Module)(nil)
	_ registry.GRPCRegistrar    = (*Module)(nil)
	_ registry.BackgroundRunner = (*Module)(nil)
)
