package spans

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/indexer"
	"github.com/gin-gonic/gin"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

// Deps collects everything NewModule needs. Producer + PersistenceClient
// come from infra; TopicPrefix drives the DLQ topic name; IndexerConfig
// overrides the trace-assembly defaults from config.yml.
type Deps struct {
	Producer          *Producer
	CH                registry.ClickHouseConn
	PersistenceClient *kafkainfra.Consumer
	KafkaBase         *kafkainfra.Producer
	TopicPrefix       string
	IndexerConfig     indexer.Config
}

// NewModule wires handler + persistence dispatcher + trace-assembly indexer.
// The indexer emits per-trace summary rows into observability.traces_index
// once each trace completes (root seen + quiet) or times out (truncated=true).
func NewModule(d Deps) registry.Module {
	dlq := NewDLQProducer(d.KafkaBase, d.TopicPrefix)
	emitter := indexer.NewCHEmitter(d.CH)
	cfg := d.IndexerConfig
	if cfg.Capacity == 0 && cfg.QuietWindow == 0 && cfg.HardTimeout == 0 && cfg.SweepEvery == 0 {
		cfg = indexer.DefaultConfig()
	}
	asm := indexer.New(emitter, cfg)
	disp := NewDispatcher(d.PersistenceClient, d.CH, dlq, asm)
	return &Module{
		handler:    NewHandler(d.Producer),
		dispatcher: disp,
		assembler:  asm,
	}
}

type Module struct {
	handler    *Handler
	dispatcher *Dispatcher
	assembler  *indexer.Assembler
}

func (m *Module) Name() string                      { return "spans" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	tracepb.RegisterTraceServiceServer(srv, m.handler)
}

func (m *Module) Start() {
	m.assembler.Start()
	m.dispatcher.Start()
}

func (m *Module) Stop() error {
	// Drain order: stop intake first so no new spans arrive, then drain the
	// assembler so pending traces emit before the CH pool closes.
	if err := m.dispatcher.Stop(); err != nil {
		return err
	}
	return m.assembler.Stop()
}

var (
	_ registry.Module           = (*Module)(nil)
	_ registry.GRPCRegistrar    = (*Module)(nil)
	_ registry.BackgroundRunner = (*Module)(nil)
)
