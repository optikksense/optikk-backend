package monitors

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gin-gonic/gin"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/query"
)

// Module wires the monitors endpoints into the v1 router. CRUD + state actions
// (ack/mute/unmute) + per-monitor reads (events, series, status-timeline) +
// the team-wide activity feed. The CH conn is needed for test/series/timeline.
type Module struct {
	handler *Handler
}

func NewModule(sqlDB *registry.SQLDB, getTenant registry.GetTenantFunc, chConn clickhouse.Conn) *Module {
	repo := NewRepository(sqlDB)
	svc := NewService(repo)
	queries := query.Registry{
		Metric: query.NewMetricBackend(chConn),
		APM:    query.NewAPMBackend(chConn),
		Log:    query.NewLogBackend(chConn),
	}
	return &Module{handler: NewHandler(getTenant, svc, queries)}
}

func (m *Module) Name() string { return "alerting.monitors" }

func (m *Module) RegisterRoutes(v1 *gin.RouterGroup) {
	g := v1.Group("/monitors")
	g.GET("", m.handler.List)
	g.POST("", m.handler.Create)
	g.GET("/activity", m.handler.Activity)
	g.GET("/:id", m.handler.Get)
	g.PUT("/:id", m.handler.Update)
	g.DELETE("/:id", m.handler.Delete)
	g.POST("/:id/ack", m.handler.Ack)
	g.POST("/:id/mute", m.handler.Mute)
	g.POST("/:id/unmute", m.handler.Unmute)
	g.POST("/:id/test", m.handler.Test)
	g.GET("/:id/events", m.handler.Events)
	g.GET("/:id/series", m.handler.Series)
	g.GET("/:id/status-timeline", m.handler.StatusTimeline)
}
