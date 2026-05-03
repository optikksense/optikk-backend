package logs

import (
	"context"
	"sync"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
)

type Deps struct {
	Handler  *Handler
	Consumer *Consumer
}

func NewModule(d Deps) registry.Module {
	return &Module{handler: d.Handler, consumer: d.Consumer}
}

type Module struct {
	handler  *Handler
	consumer *Consumer

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (m *Module) Name() string                      { return "logs" }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) RegisterGRPC(srv *grpc.Server) {
	logspb.RegisterLogsServiceServer(srv, m.handler)
}

func (m *Module) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.consumer.Run(ctx)
	}()
}

func (m *Module) Stop() error {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
	return nil
}

var (
	_ registry.Module           = (*Module)(nil)
	_ registry.GRPCRegistrar    = (*Module)(nil)
	_ registry.BackgroundRunner = (*Module)(nil)
)
