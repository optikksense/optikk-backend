package metrics

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
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
)

type Config struct {
	Auth          ingestion.TeamResolver
	Tracker       ingestion.SizeTracker
	CH            clickhouse.Conn
	KafkaBrokers  []string
	ConsumerGroup string
	Topic         string
}

func NewModule(deps *registry.Deps) (registry.Module, error) {
	cfg := Config{
		Auth:          deps.TeamResolver,
		Tracker:       deps.SizeTracker,
		CH:            deps.CH,
		KafkaBrokers:  deps.KafkaBrokers,
		ConsumerGroup: deps.ConsumerGroup,
		Topic:         deps.TopicPrefix + ".metrics",
	}

	producer, _, persistClient, err := kafka.InitIngestClients(cfg.KafkaBrokers, cfg.ConsumerGroup, cfg.Topic, "metrics")
	if err != nil {
		return nil, fmt.Errorf("otlp metrics kafka: %w", err)
	}

	flusher := dbutil.NewCHFlusher[*MetricRow](cfg.CH, "observability.metrics", MetricColumns)
	dispatcher := kafka.NewDispatcher[*proto.MetricRow](producer, cfg.Topic, "metrics")

	return &Module{
		service:    NewService(cfg.Auth, dispatcher, cfg.Tracker),
		persist:    NewPersistenceConsumer(persistClient, flusher),
		dispatcher: dispatcher,
	}, nil
}

type Module struct {
	service    *Service
	persist    *PersistenceConsumer
	dispatcher *kafka.Dispatcher[*proto.MetricRow]
}

func (m *Module) Name() string                      { return "otlpMetrics" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	metricspb.RegisterMetricsServiceServer(srv, m.service)
}

func (m *Module) Start() {
	if m.persist != nil {
		m.persist.Start(context.Background())
	}
}

func (m *Module) Stop() error {
	if m.persist != nil {
		_ = m.persist.Stop()
	}
	if m.dispatcher != nil {
		m.dispatcher.Close()
	}
	return nil
}
