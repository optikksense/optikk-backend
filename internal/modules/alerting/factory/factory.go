// Package factory composes the decomposed alerting subsystem into the set
// of registry.Module entries consumed by modules_manifest.go. Kept in its own
// package so the import graph stays one-directional:
//
//   factory → rules, incidents, silences, slack, engine
//   rules, incidents, silences, slack, engine → alerting (types + helpers)
//
// If this were a function on the parent alerting package, the parent would
// have to import each submodule, which would forbid submodules from
// importing the parent for shared types.
package factory

import (
	"database/sql"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/engine"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/evaluators"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/incidents"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/rules"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/silences"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/slack"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	goredis "github.com/redis/go-redis/v9"
)

// NewModules wires the alerting subsystem and returns the five modules that
// the manifest registers:
//
//   rules      → /alerts/rules/*
//   incidents  → /alerts/incidents, /alerts/activity, /alerts/instances/:id/*
//   silences   → /alerts/silences/*
//   slack      → /alerts/slack/test, /alerts/callback/slack
//   engine     → BackgroundRunner (evaluator loop + dispatcher + outbox relay)
func NewModules(
	db *sql.DB,
	nativeQuerier *dbutil.NativeQuerier,
	chConn clickhouse.Conn,
	redisClient *goredis.Client,
	getTenant registry.GetTenantFunc,
	baseURL string,
	maxEnabledRules int,
) []registry.Module {
	ruleRepo := rules.NewRepository(db, maxEnabledRules)
	chStore := engine.NewStore(nativeQuerier, chConn)
	outbox := engine.NewOutboxStore(db)
	dispatcher := engine.NewDispatcher(chStore, baseURL)
	dispatcher.SetOutbox(outbox)
	relay := engine.NewOutboxRelay(outbox)
	lease := engine.NewLeaser(redisClient, 60*time.Second)

	reg := evaluators.NewRegistry(
		&evaluators.SLOBurnRate{Data: chStore},
		&evaluators.ErrorRate{Data: chStore},
		&evaluators.AILatency{Data: chStore},
		&evaluators.AIErrorRate{Data: chStore},
		&evaluators.AICostSpike{Data: chStore},
		&evaluators.AIQualityDrop{Data: chStore},
		&evaluators.HTTPCheck{},
	)
	backtester := engine.NewBacktester(reg)
	loop := engine.NewEvaluatorLoop(ruleRepo, chStore, chStore, reg, dispatcher, lease, outbox)
	runner := engine.NewRunner(loop, dispatcher, relay)

	tenant := modulecommon.DBTenant{DB: db, GetTenant: getTenant}

	rulesHandler := &rules.Handler{DBTenant: tenant, Service: rules.NewService(ruleRepo, chStore, reg, backtester)}
	incidentsHandler := &incidents.Handler{DBTenant: tenant, Service: incidents.NewService(ruleRepo, chStore)}
	silencesHandler := &silences.Handler{DBTenant: tenant, Service: silences.NewService(ruleRepo, chStore)}
	slackHandler := &slack.Handler{DBTenant: tenant, Service: slack.NewService(dispatcher)}

	return []registry.Module{
		rules.NewModule(rulesHandler),
		incidents.NewModule(incidentsHandler),
		silences.NewModule(silencesHandler),
		slack.NewModule(slackHandler),
		engine.NewModule(runner),
	}
}
