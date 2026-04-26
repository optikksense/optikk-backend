// Package module wires the logs signal's handler + dispatcher + DLQ into a
// registry.Module ready for Fx registration. Every signal has its own module
// package so the four flat layers (schema, mapper, ingress, consumer, dlq)
// stay independent.
package module

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	kconsumer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/consumer"
	kproducer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/producer"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/consumer"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/dlq"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/ingress"
	"github.com/gin-gonic/gin"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
)

// Deps collects everything NewModule needs. Pipeline drives the generic
// Dispatcher/Worker/Writer tuning knobs.
type Deps struct {
	Producer          *ingress.Producer
	CH                registry.ClickHouseConn
	PersistenceClient *kconsumer.Consumer
	KafkaBase         *kproducer.Producer
	TopicPrefix       string
	Pipeline          config.IngestPipelineConfig
}

// NewModule wires the handler + dispatcher (generic PollFetches + per-partition
// worker + retrying CH writer + DLQ).
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

func (m *Module) Name() string                      { return "logs" }
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
