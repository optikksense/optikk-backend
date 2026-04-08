package logs

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	platformingestion "github.com/Optikk-Org/optikk-backend/internal/platform/ingestion"
	"github.com/gin-gonic/gin"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
)

func NewModule(authenticator platformingestion.TeamResolver, tracker platformingestion.SizeTracker, limiter platformingestion.Limiter, d platformingestion.Dispatcher[*LogRow]) registry.Module {
	service := NewService(authenticator, d, tracker, limiter)
	return &Module{
		handler: NewHandler(service),
	}
}

type Module struct {
	handler *Handler
}

func (m *Module) Name() string                      { return "otlpLogs" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	logspb.RegisterLogsServiceServer(srv, m.handler)
}

func (m *Module) Start() {}

func (m *Module) Stop() error { return nil }
