package metrics

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
	"github.com/gin-gonic/gin"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
)

// Deps collects everything NewModule needs. The metric signal has no UI
// live-tail surface today, so there is no LivetailClient here — if a live
// metrics tail ships later, add it alongside PersistenceClient without
// touching the rest of the module.
type Deps struct {
	Producer          *Producer
	CH                registry.ClickHouseConn
	PersistenceClient *kafkainfra.Consumer
	SketchStore       sketch.Store
}

// NewModule wires the handler and persistence consumer into a single
// registry.Module.
func NewModule(d Deps) registry.Module {
	return &Module{
		handler:  NewHandler(d.Producer),
		consumer: NewConsumer(d.PersistenceClient, d.CH, d.SketchStore),
	}
}

type Module struct {
	handler  *Handler
	consumer *Consumer
}

func (m *Module) Name() string                      { return "metrics-ingest" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	metricspb.RegisterMetricsServiceServer(srv, m.handler)
}

func (m *Module) Start()       { m.consumer.Start() }
func (m *Module) Stop() error  { return m.consumer.Stop() }

var (
	_ registry.Module           = (*Module)(nil)
	_ registry.GRPCRegistrar    = (*Module)(nil)
	_ registry.BackgroundRunner = (*Module)(nil)
)
