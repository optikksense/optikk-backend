package alerting

import (
	"database/sql"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/evaluators"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Module bundles the HTTP router registration plus the evaluator and
// dispatcher background runners. It implements both registry.Module and
// registry.BackgroundRunner so the manifest wiring stays symmetric with
// otlp streamworkers.
type Module struct {
	handler    *Handler
	evaluator  *EvaluatorLoop
	dispatcher *Dispatcher
}

// NewModule wires the full alerting stack. Rules live in MySQL, events in
// ClickHouse, and evaluator queries go through the native querier.
func NewModule(
	db *sql.DB,
	nativeQuerier *registry.NativeQuerier,
	chConn registry.ClickHouseConn,
	getTenant registry.GetTenantFunc,
	baseURL string,
) registry.Module {
	repo := NewRepository(db, nativeQuerier, chConn)
	reg := evaluators.NewRegistry(
		&evaluators.SLOBurnRate{Data: repo},
		&evaluators.ErrorRate{Data: repo},
		&evaluators.AILatency{Data: repo},
		&evaluators.AIErrorRate{Data: repo},
		&evaluators.AICostSpike{Data: repo},
		&evaluators.AIQualityDrop{Data: repo},
		&evaluators.HTTPCheck{},
	)
	dispatcher := NewDispatcher(repo, baseURL)
	svc := NewService(repo, reg, dispatcher)
	loop := NewEvaluatorLoop(repo, reg, dispatcher)

	return &Module{
		handler: &Handler{
			DBTenant: modulecommon.DBTenant{DB: db, GetTenant: getTenant},
			Service:  svc,
		},
		evaluator:  loop,
		dispatcher: dispatcher,
	}
}

func (m *Module) Name() string                      { return "alerting" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *Module) RegisterRoutes(group *gin.RouterGroup) {
	a := group.Group("/alerts")
	a.POST("/rules", m.handler.CreateRule)
	a.GET("/rules", m.handler.ListRules)
	a.GET("/rules/:id", m.handler.GetRule)
	a.PATCH("/rules/:id", m.handler.UpdateRule)
	a.DELETE("/rules/:id", m.handler.DeleteRule)

	a.POST("/rules/:id/mute", m.handler.MuteRule)
	a.POST("/rules/:id/test", m.handler.TestRule)
	a.POST("/rules/:id/backtest", m.handler.BacktestRule)
	a.GET("/rules/:id/audit", m.handler.ListAudit)

	a.GET("/incidents", m.handler.ListIncidents)

	a.POST("/instances/:id/ack", m.handler.AckInstance)
	a.POST("/instances/:id/snooze", m.handler.SnoozeInstance)

	a.GET("/silences", m.handler.ListSilences)
	a.POST("/silences", m.handler.CreateSilence)
	a.PATCH("/silences/:id", m.handler.UpdateSilence)
	a.DELETE("/silences/:id", m.handler.DeleteSilence)

	a.POST("/callback/slack", m.handler.SlackCallback)
}

// Start implements registry.BackgroundRunner — boots the evaluator tick loop
// and the dispatcher consumer goroutine.
func (m *Module) Start() {
	m.dispatcher.Start()
	m.evaluator.Start()
}

// Stop implements registry.BackgroundRunner — drains both runners.
func (m *Module) Stop() error {
	if err := m.evaluator.Stop(); err != nil {
		return err
	}
	return m.dispatcher.Stop()
}

var (
	_ registry.Module           = (*Module)(nil)
	_ registry.BackgroundRunner = (*Module)(nil)
)
