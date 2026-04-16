package metrics

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/kafkadispatcher"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/proto"
	"github.com/gin-gonic/gin"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
)

func NewModule(
	authenticator ingestion.TeamResolver,
	tracker ingestion.SizeTracker,
	d *kafkadispatcher.Dispatcher[*proto.MetricRow],
	persist *PersistenceConsumer,
) registry.Module {
	return &Module{
		service: NewService(authenticator, d, tracker),
		persist: persist,
	}
}

type Module struct {
	service *Service
	persist *PersistenceConsumer
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
		return m.persist.Stop()
	}
	return nil
}
