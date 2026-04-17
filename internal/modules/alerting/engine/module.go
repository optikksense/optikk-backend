// Package engine carries the alerting background runtime — the evaluator
// tick loop, the in-memory dispatcher, the durable outbox relay — plus the
// ClickHouse-side persistence (event store, evaluator data source) and the
// backtest runner. Owns the registry.BackgroundRunner lifecycle; has no HTTP
// routes.
package engine

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

// Module wraps the Runner (evaluator loop, dispatcher, outbox relay) as a
// registry.BackgroundRunner so the manifest binds one Start/Stop to the group.
type Module struct {
	runner *Runner
}

func NewModule(runner *Runner) registry.Module {
	return &Module{runner: runner}
}

func (m *Module) Name() string                      { return "alerting.engine" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }
func (m *Module) RegisterRoutes(_ *gin.RouterGroup) {}

func (m *Module) Start()      { m.runner.Start() }
func (m *Module) Stop() error { return m.runner.Stop() }

var (
	_ registry.Module           = (*Module)(nil)
	_ registry.BackgroundRunner = (*Module)(nil)
)
