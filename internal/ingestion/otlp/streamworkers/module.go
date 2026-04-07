package streamworkers

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	otlplogs "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/logs"
	otlpmetrics "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/metrics"
	otlpspans "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/spans"
	platformingestion "github.com/Optikk-Org/optikk-backend/internal/platform/ingestion"
	platformlivetail "github.com/Optikk-Org/optikk-backend/internal/platform/livetail"
	"github.com/gin-gonic/gin"
)

// Module runs OTLP background consumers (BackgroundRunner).
type Module struct {
	workers *Workers
}

// NewModule wires ClickHouse + Hub consumers.
func NewModule(
	ch registry.ClickHouseConn,
	ld platformingestion.Dispatcher[*otlplogs.LogRow],
	sd platformingestion.Dispatcher[*otlpspans.SpanRow],
	md platformingestion.Dispatcher[*otlpmetrics.MetricRow],
	hub platformlivetail.Hub,
) registry.Module {
	return &Module{workers: NewWorkers(ch, ld, sd, md, hub)}
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
	_ registry.Module           = (*Module)(nil)
	_ registry.BackgroundRunner = (*Module)(nil)
)
