package spans

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	"github.com/gin-gonic/gin"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

func NewModule(sqlDB *registry.SQLDB, appConfig registry.AppConfig, d *otlp.Dispatcher) registry.Module {
	shared := otlp.Shared(sqlDB, appConfig)
	service := NewService(shared.Authenticator, d, shared.Tracker, shared.Limiter)

	return &Module{
		handler:   NewHandler(service),
		lifecycle: otlp.NewLifecycle(shared),
	}
}

type Module struct {
	handler   *Handler
	lifecycle otlp.Lifecycle
}

func (m *Module) Name() string                      { return "otlpSpans" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	tracepb.RegisterTraceServiceServer(srv, m.handler)
}

func (m *Module) Start() {}

func (m *Module) Stop() error {
	m.lifecycle.Stop()
	return nil
}
