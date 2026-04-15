package spans

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/gin-gonic/gin"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

func NewModule(authenticator ingestion.TeamResolver, tracker ingestion.SizeTracker, d ingestion.Dispatcher[*SpanRow]) registry.Module {
	service := NewService(authenticator, d, tracker)
	return &Module{
		handler: NewHandler(service),
	}
}

type Module struct {
	handler *Handler
}

func (m *Module) Name() string                      { return "otlpSpans" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	tracepb.RegisterTraceServiceServer(srv, m.handler)
}

func (m *Module) Start() {}

func (m *Module) Stop() error { return nil }
