// Package trace_logs owns GET /api/v1/logs/trace/:traceID — fetch all logs
// for a trace via the observability.trace_index reverse-projection table.
// Replaces the old tracedetail GET /traces/:traceId/logs route.
package trace_logs

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

func RegisterRoutes(v1 *gin.RouterGroup, h *Handler) {
	if h == nil {
		return
	}
	v1.GET("/logs/trace/:traceID", h.GetByTrace)
}

func NewModule(db clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	m := &module{}
	m.configure(db, getTenant)
	return m
}

type module struct {
	handler *Handler
}

func (m *module) Name() string { return "logsTraceLogs" }

func (m *module) configure(db clickhouse.Conn, getTenant registry.GetTenantFunc) {
	repo := NewRepository(db)
	svc := NewService(repo)
	m.handler = NewHandler(getTenant, svc)
}

func (m *module) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(group, m.handler)
}

var _ modulecommon.GetTenantFunc = modulecommon.GetTenantFunc(nil)
