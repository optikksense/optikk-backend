// Package factory composes the AI subsystem into discrete registry.Module
// entries consumed by modules_manifest.go. Mirrors the alerting/factory
// pattern: lives outside the parent ai package so the import graph stays
// one-directional (factory → subpackages; subpackages → parent for shared
// Service + Repository).
package factory

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/modules/ai/shared"
	ai_analytics "github.com/Optikk-Org/optikk-backend/internal/modules/ai/analytics"
	ai_overview "github.com/Optikk-Org/optikk-backend/internal/modules/ai/overview"
	ai_runs "github.com/Optikk-Org/optikk-backend/internal/modules/ai/runs"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
)

// NewModules wires the three AI submodules and returns them for manifest
// registration. The legacy ai.Handler god file has been removed; parent ai
// now exposes the shared Service + Repository and the submodules own their
// own handlers.
func NewModules(
	nativeQuerier clickhouse.Conn,
	sqlDB *registry.SQLDB,
	getTenant registry.GetTenantFunc,
) []registry.Module {
	repo := shared.NewRepository(nativeQuerier, sqlDB)
	svc := shared.NewService(repo)
	tenant := modulecommon.DBTenant{GetTenant: getTenant}

	return []registry.Module{
		ai_overview.NewModule(&ai_overview.Handler{DBTenant: tenant, Service: svc}),
		ai_runs.NewModule(&ai_runs.Handler{DBTenant: tenant, Service: svc}),
		ai_analytics.NewModule(&ai_analytics.Handler{DBTenant: tenant, Service: svc}),
	}
}
