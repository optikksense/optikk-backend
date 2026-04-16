package spans

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/kafkadispatcher"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/proto"
	"github.com/gin-gonic/gin"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

func NewModule(
	authenticator ingestion.TeamResolver,
	tracker ingestion.SizeTracker,
	d *kafkadispatcher.Dispatcher[*proto.SpanRow],
	persist *PersistenceConsumer,
	stream *StreamingConsumer,
) registry.Module {
	return &Module{
		service: NewService(authenticator, d, tracker),
		persist: persist,
		stream:  stream,
	}
}

type Module struct {
	service *Service
	persist *PersistenceConsumer
	stream  *StreamingConsumer
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
	return nil
}
