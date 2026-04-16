package logs

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/proto"
	"github.com/gin-gonic/gin"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
)

type Config struct {
	Auth          ingestion.TeamResolver
	Tracker       ingestion.SizeTracker
	CH            clickhouse.Conn
	Hub           registry.LiveTailHub
	KafkaBrokers  []string
	ConsumerGroup string
	Topic         string
}

func NewModule(deps *registry.Deps) (registry.Module, error) {
	cfg := Config{
		Auth:          deps.TeamResolver,
		Tracker:       deps.SizeTracker,
		CH:            deps.CH,
		Hub:           deps.LiveTailHub,
		KafkaBrokers:  deps.KafkaBrokers,
		ConsumerGroup: deps.ConsumerGroup,
		Topic:         deps.TopicPrefix + ".logs",
	}

	producer, streamClient, persistClient, err := kafka.InitIngestClients(cfg.KafkaBrokers, cfg.ConsumerGroup, cfg.Topic, "logs")
	if err != nil {
		return nil, fmt.Errorf("otlp logs kafka: %w", err)
	}

	flusher := otlp.NewCHFlusher[*LogRow](cfg.CH, "observability.logs", LogColumns)
	dispatcher := kafka.NewDispatcher[*proto.LogRow](producer, cfg.Topic, "logs")

	return &Module{
		service:    NewService(cfg.Auth, dispatcher, cfg.Tracker),
		persist:    NewPersistenceConsumer(persistClient, flusher),
		stream:     NewStreamingConsumer(streamClient, cfg.Hub),
		dispatcher: dispatcher,
	}, nil
}

type Module struct {
	service    *Service
	persist    *PersistenceConsumer
	stream     *StreamingConsumer
	dispatcher *kafka.Dispatcher[*proto.LogRow]
}

func (m *Module) Name() string                      { return "otlpLogs" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	logspb.RegisterLogsServiceServer(srv, m.service)
}

func (m *Module) Start() {
	ctx := context.Background()
	if m.persist != nil {
		m.persist.Start(ctx)
	}
	if m.stream != nil {
		m.stream.Start(ctx)
	}
}

func (m *Module) Stop() error {
	if m.persist != nil {
		_ = m.persist.Stop()
	}
	if m.stream != nil {
		_ = m.stream.Stop()
	}
	if m.dispatcher != nil {
		m.dispatcher.Close()
	}
	return nil
}
