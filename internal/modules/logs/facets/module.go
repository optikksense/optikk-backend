// Package log_facets provides top-N facet buckets per dimension.
// It is also exposed as a Service method for external callers.
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
