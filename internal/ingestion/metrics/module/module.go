// Package module wires the metrics signal's handler + dispatcher + DLQ into
// a registry.Module ready for Fx registration.
package module

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	kconsumer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/consumer"
	kproducer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/producer"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/consumer"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/dlq"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/ingress"
	"github.com/gin-gonic/gin"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
)

// Deps collects everything NewModule needs. Pipeline drives the generic
// Dispatcher/Worker/Writer tuning knobs. KafkaBase + TopicPrefix feed the
// DLQ producer (new in this round — the legacy consumer had none).
type Deps struct {
	Producer          *ingress.Producer
	CH                registry.ClickHouseConn
	PersistenceClient *kconsumer.Consumer
	KafkaBase         *kproducer.Producer
	TopicPrefix       string
	Pipeline          config.IngestPipelineConfig
}

// NewModule wires handler + dispatcher (generic PollFetches + per-partition
// worker + retrying CH writer + DLQ). Replaces the legacy single-goroutine
// Consumer — metrics now joins logs + spans on the shared pipeline.
func NewModule(d Deps) registry.Module {
	dlqP := dlq.NewProducer(d.KafkaBase, d.TopicPrefix)
	return &Module{
		handler:    ingress.NewHandler(d.Producer),
		dispatcher: consumer.NewDispatcher(d.PersistenceClient, d.CH, dlqP, d.Pipeline),
	}
}

type Module struct {
	handler    *ingress.Handler
	dispatcher *consumer.Dispatcher
}

func (m *Module) Name() string                      { return "metrics-ingest" }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	metricspb.RegisterMetricsServiceServer(srv, m.handler)
}

func (m *Module) Start()      { m.dispatcher.Start() }
func (m *Module) Stop() error { return m.dispatcher.Stop() }

var (
	_ registry.Module           = (*Module)(nil)
	_ registry.GRPCRegistrar    = (*Module)(nil)
	_ registry.BackgroundRunner = (*Module)(nil)
)
