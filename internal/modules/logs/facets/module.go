// Package log_facets owns POST /api/v1/logs/facets — top-N facet buckets
// per dimension (severity, service, host, pod, environment). Also exposed
// as a Service method so the explorer aggregator can fan it as an include.
package log_facets //nolint:revive,stylecheck

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

func RegisterRoutes(v1 *gin.RouterGroup, h *Handler) {
	v1.POST("/logs/facets", h.Facets)
}

func NewModule(db clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	m := &module{}
	m.configure(db, getTenant)
	return m
}

// NewService builds a wired Service so explorer can compose facets without
// going through the HTTP surface.
func NewServiceFromDB(db clickhouse.Conn) *Service {
	return NewService(NewRepository(db))
}

type module struct {
	handler *Handler
}

func (m *module) Name() string { return "logsFacets" }

func (m *module) configure(db clickhouse.Conn, getTenant registry.GetTenantFunc) {
	repo := NewRepository(db)
	svc := NewService(repo)
	m.handler = NewHandler(getTenant, svc)
}

func (m *module) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(group, m.handler)
}

var _ modulecommon.GetTenantFunc = modulecommon.GetTenantFunc(nil)
