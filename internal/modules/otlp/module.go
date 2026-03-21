package otlp

import (
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/otlp/auth"
	"github.com/observability/observability-backend-go/internal/modules/otlp/internal/ingest"
	"github.com/observability/observability-backend-go/internal/modules/otlp/internal/mapper"
	"github.com/observability/observability-backend-go/internal/modules/registry"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func init() {
	registry.Register(&Module{})
}

type Module struct {
	handler      *Handler
	service      *Service
	spansQueue   *ingest.Queue
	logsQueue    *ingest.Queue
	metricsQueue *ingest.Queue
	tracker      *ingest.ByteTracker
}

func (m *Module) Name() string                      { return "otlp" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *Module) Init(deps registry.Deps) error {
	cfg := deps.Config

	queueOpts := []ingest.Option{
		ingest.WithBatchSize(cfg.Queue.BatchSize),
		ingest.WithFlushInterval(int(cfg.Queue.FlushIntervalMs)),
	}

	m.spansQueue = ingest.NewQueue(deps.ClickHouseConn, "observability.spans", mapper.SpanColumns, queueOpts...)
	m.logsQueue = ingest.NewQueue(deps.ClickHouseConn, "observability.logs", mapper.LogColumns, queueOpts...)
	m.metricsQueue = ingest.NewQueue(deps.ClickHouseConn, "observability.metrics", mapper.MetricColumns, queueOpts...)

	authenticator := auth.NewAuthenticator(deps.DB)
	m.tracker = ingest.NewByteTracker(deps.DB, time.Hour)
	limiter := ingest.NewTeamLimiter(ingest.DefaultTeamRatePerSec, ingest.DefaultTeamBurstRows)

	m.service = NewService(authenticator, m.spansQueue, m.logsQueue, m.metricsQueue, m.tracker, limiter)
	m.handler = NewHandler(m.service)

	return nil
}

// RegisterRoutes is a no-op — OTLP ingest is exposed only over gRPC.
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

// RegisterGRPC registers OTLP gRPC services on the shared gRPC server.
func (m *Module) RegisterGRPC(srv *grpc.Server) {
	tracepb.RegisterTraceServiceServer(srv, m.handler.TraceServer)
	logspb.RegisterLogsServiceServer(srv, m.handler.LogsServer)
	metricspb.RegisterMetricsServiceServer(srv, m.handler.MetricsServer)
	reflection.Register(srv)
}

// Start launches background workers (byte tracker).
func (m *Module) Start() {
	m.tracker.Start()
}

// Stop drains ingest queues and stops the byte tracker.
func (m *Module) Stop() error {
	for _, q := range []*ingest.Queue{m.spansQueue, m.logsQueue, m.metricsQueue} {
		if q != nil {
			if err := q.Close(); err != nil {
				log.Printf("WARN: error flushing ingest queue: %v", err)
			}
		}
	}
	if m.tracker != nil {
		m.tracker.Stop()
	}
	return nil
}
