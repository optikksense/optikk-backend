// Package module wires the spans signal's handler + dispatcher + DLQ into a
// registry.Module ready for Fx registration. The trace assembler is optional
// now that the read path no longer depends on per-trace access-path tables.
package module

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	kconsumer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/consumer"
	kproducer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/producer"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/consumer"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/dlq"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/ingress"
	"github.com/gin-gonic/gin"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

// Deps collects everything NewModule needs. Producer + PersistenceClient
// come from infra; TopicPrefix drives the DLQ topic name; IndexerConfig
// overrides the trace-assembly defaults from config.yml; Pipeline tunes
// the generic ingest pipeline (batching, retry, async_insert).
type Deps struct {
	Producer          *ingress.Producer
	CH                registry.ClickHouseConn
	PersistenceClient *kconsumer.Consumer
	KafkaBase         *kproducer.Producer
	TopicPrefix       string
	Pipeline          config.IngestPipelineConfig
}

// NewModule wires handler + persistence dispatcher. The legacy trace
// assembler is intentionally disabled for the raw-table path.
func NewModule(d Deps) registry.Module {
	dlqP := dlq.NewProducer(d.KafkaBase, d.TopicPrefix)
	disp := consumer.NewDispatcher(d.PersistenceClient, d.CH, dlqP, d.Pipeline)
	return &Module{
		handler:    ingress.NewHandler(d.Producer),
		dispatcher: disp,
	}
}

type Module struct {
	handler    *ingress.Handler
	dispatcher *consumer.Dispatcher
}

func (m *Module) Name() string                      { return "spans" }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	tracepb.RegisterTraceServiceServer(srv, m.handler)
}

func (m *Module) Start() {
	m.dispatcher.Start()
}

func (m *Module) Stop() error {
	// Drain order: stop intake first so no new spans arrive.
	return m.dispatcher.Stop()
}

var (
	_ registry.Module           = (*Module)(nil)
	_ registry.GRPCRegistrar    = (*Module)(nil)
	_ registry.BackgroundRunner = (*Module)(nil)
)
