package streamworkers

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/Optikk-Org/optikk-backend/internal/infra/livetail"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	"github.com/gin-gonic/gin"
)

// Module runs OTLP background consumers (BackgroundRunner).
type Module struct {
	workers *Workers
}

// NewModule wires ClickHouse + Hub consumers.
func NewModule(ch registry.ClickHouseConn, d *otlp.Dispatcher, hub *livetail.Hub, cfg config.Config) registry.Module {
	return &Module{workers: NewWorkers(ch, d, hub, cfg)}
}

func (m *Module) Name() string { return "otlpStreamWorkers" }

func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) Start() {
	m.workers.Start()
}

func (m *Module) Stop() error {
	return m.workers.Stop()
}

var (
	_ registry.Module          = (*Module)(nil)
	_ registry.BackgroundRunner = (*Module)(nil)
)
