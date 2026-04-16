package spans

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/proto"
	"github.com/gin-gonic/gin"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
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
		Topic:         deps.TopicPrefix + ".spans",
	}

	producer, streamClient, persistClient, err := kafka.InitIngestClients(cfg.KafkaBrokers, cfg.ConsumerGroup, cfg.Topic, "spans")
	if err != nil {
		return nil, fmt.Errorf("otlp spans kafka: %w", err)
	}

	flusher := dbutil.NewCHFlusher[*SpanRow](cfg.CH, "observability.spans", SpanColumns)
	dispatcher := kafka.NewDispatcher[*proto.SpanRow](producer, cfg.Topic, "spans")

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
	dispatcher *kafka.Dispatcher[*proto.SpanRow]
}

func (m *Module) Name() string                      { return "otlpSpans" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	tracepb.RegisterTraceServiceServer(srv, m.service)
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
