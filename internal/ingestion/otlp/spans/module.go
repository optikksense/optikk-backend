package spans

import (
	"context"
	"log/slog"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
	serviceinventory "github.com/Optikk-Org/optikk-backend/internal/modules/services/inventory"
	"github.com/gin-gonic/gin"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

func NewModule(sqlDB *registry.SQLDB, nativeQuerier *registry.NativeQuerier, clickHouseConn registry.ClickHouseConn, appConfig registry.AppConfig) registry.Module {
	shared := otlp.Shared(sqlDB, appConfig)
	flusher := otlp.NewCHFlusher(clickHouseConn, "observability.spans", spanColumns)
	queue := ingest.NewQueue(flusher.Flush, otlp.QueueOpts(appConfig)...)
	inventoryRepo := serviceinventory.NewRepository(sqlDB, appConfig)
	projector := serviceinventory.NewProjector(
		inventoryRepo,
		serviceinventory.NewBootstrapRepository(nativeQuerier),
	)
	service := NewService(shared.Authenticator, queue, shared.Tracker, shared.Limiter, projector)

	return &Module{
		handler:   NewHandler(service),
		queue:     queue,
		lifecycle: otlp.NewLifecycle(shared),
		projector: projector,
	}
}

type Module struct {
	handler   *Handler
	queue     otlp.Queue
	lifecycle otlp.Lifecycle
	projector *serviceinventory.Projector
}

func (m *Module) Name() string                      { return "otlpSpans" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	tracepb.RegisterTraceServiceServer(srv, m.handler)
}

func (m *Module) Start() {
	if m.projector == nil {
		return
	}
	if err := m.projector.BootstrapFromHistory(context.Background()); err != nil {
		logger.L().Warn("service inventory bootstrap failed", slog.Any("error", err))
	}
}

func (m *Module) Stop() error {
	if m.queue != nil {
		if err := m.queue.Close(); err != nil {
			logger.L().Warn("error flushing ingest queue", slog.Any("error", err))
		}
	}
	m.lifecycle.Stop()
	return nil
}
