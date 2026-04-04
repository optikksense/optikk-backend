package metrics

import (
	"log/slog"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
	"github.com/gin-gonic/gin"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
)

func NewModule(sqlDB *registry.SQLDB, clickHouseConn registry.ClickHouseConn, appConfig registry.AppConfig) registry.Module {
	shared := otlp.Shared(sqlDB, appConfig)
	flusher := otlp.NewCHFlusher(clickHouseConn, "observability.metrics", metricColumns)
	queue := ingest.NewQueue(flusher.Flush, otlp.QueueOpts(appConfig)...)
	service := NewService(shared.Authenticator, queue, shared.Tracker, shared.Limiter)

	return &Module{
		handler:   NewHandler(service),
		queue:     queue,
		lifecycle: otlp.NewLifecycle(shared),
	}
}

type Module struct {
	handler   *Handler
	queue     otlp.Queue
	lifecycle otlp.Lifecycle
}

func (m *Module) Name() string                      { return "otlpMetrics" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	metricspb.RegisterMetricsServiceServer(srv, m.handler)
}

func (m *Module) Start() {}

func (m *Module) Stop() error {
	if m.queue != nil {
		if err := m.queue.Close(); err != nil {
			slog.Warn("error flushing ingest queue", slog.Any("error", err))
		}
	}
	m.lifecycle.Stop()
	return nil
}
