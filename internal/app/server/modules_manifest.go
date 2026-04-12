package server

import (
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	otlp_logs "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/logs"
	otlp_metrics "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/metrics"
	otlp_spans "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/spans"
	otlp_streamworkers "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/streamworkers"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting"

	infrastructure_connpool "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/connpool"
	infrastructure_cpu "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/cpu"
	infrastructure_disk "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/disk"
	infrastructure_jvm "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/jvm"
	infrastructure_kubernetes "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/kubernetes"
	infrastructure_memory "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/memory"
	infrastructure_network "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/network"
	infrastructure_fleet "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/fleet"
	infrastructure_nodes "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/nodes"
	infrastructure_resource_utilisation "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/resourceutil"
	log_explorer "github.com/Optikk-Org/optikk-backend/internal/modules/logs/explorer"
	log_search "github.com/Optikk-Org/optikk-backend/internal/modules/logs/search"
	"github.com/Optikk-Org/optikk-backend/internal/modules/metrics"
	"github.com/Optikk-Org/optikk-backend/internal/modules/overview/apm"
	overview_errors "github.com/Optikk-Org/optikk-backend/internal/modules/overview/errors"
	"github.com/Optikk-Org/optikk-backend/internal/modules/overview/httpmetrics"
	overview_overview "github.com/Optikk-Org/optikk-backend/internal/modules/overview/overview"
	overview_redmetrics "github.com/Optikk-Org/optikk-backend/internal/modules/overview/redmetrics"
	overview_slo "github.com/Optikk-Org/optikk-backend/internal/modules/overview/slo"
	saturation_database_collection "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/collection"
	saturation_database_connections "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/connections"
	saturation_database_errors "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/errors"
	saturation_explorer "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/explorer"
	saturation_database_latency "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/latency"
	saturation_database_slowqueries "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/slowqueries"
	saturation_database_summary "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/summary"
	saturation_database_system "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/system"
	saturation_database_systems "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/systems"
	saturation_database_volume "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/volume"
	saturation_kafka "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/modules/services/deployments"
	services_topology "github.com/Optikk-Org/optikk-backend/internal/modules/services/topology"
	ai_explorer "github.com/Optikk-Org/optikk-backend/internal/modules/ai/explorer"
	llm_hub "github.com/Optikk-Org/optikk-backend/internal/modules/llm/hub"
	"github.com/Optikk-Org/optikk-backend/internal/modules/livetail"
	spans_explorer "github.com/Optikk-Org/optikk-backend/internal/modules/traces/explorer"
	spans_livetail "github.com/Optikk-Org/optikk-backend/internal/modules/traces/livetail"
	spans_traces "github.com/Optikk-Org/optikk-backend/internal/modules/traces/query"
	spans_tracedetail "github.com/Optikk-Org/optikk-backend/internal/modules/traces/tracedetail"
	user_auth "github.com/Optikk-Org/optikk-backend/internal/modules/user/auth"
	user_team "github.com/Optikk-Org/optikk-backend/internal/modules/user/team"
	user_user "github.com/Optikk-Org/optikk-backend/internal/modules/user/user"
)

func configuredModules(
	nativeQuerier *registry.NativeQuerier,
	getTenant registry.GetTenantFunc,
	appConfig registry.AppConfig,
	infraDeps *Infra,
) []registry.Module {
	return []registry.Module{
		ai_explorer.NewModule(nativeQuerier, getTenant),
		llm_hub.NewModule(infraDeps.DB, getTenant),
		alerting.NewModule(infraDeps.DB, nativeQuerier, infraDeps.CH, getTenant, "", appConfig.AlertingMaxEnabledRules()),
		apm.NewModule(nativeQuerier, getTenant),
		deployments.NewModule(nativeQuerier, getTenant),
		httpmetrics.NewModule(nativeQuerier, getTenant),

		infrastructure_connpool.NewModule(nativeQuerier, getTenant),
		infrastructure_cpu.NewModule(nativeQuerier, getTenant),
		infrastructure_disk.NewModule(nativeQuerier, getTenant),
		infrastructure_jvm.NewModule(nativeQuerier, getTenant),
		infrastructure_kubernetes.NewModule(nativeQuerier, getTenant),
		infrastructure_memory.NewModule(nativeQuerier, getTenant),
		infrastructure_network.NewModule(nativeQuerier, getTenant),
		infrastructure_fleet.NewModule(nativeQuerier, getTenant),
		infrastructure_nodes.NewModule(nativeQuerier, getTenant),
		infrastructure_resource_utilisation.NewModule(nativeQuerier, getTenant),
		log_explorer.NewModule(nativeQuerier, getTenant),
		log_search.NewModule(nativeQuerier, getTenant),
		livetail.NewModule(livetail.Config{
			Hub:            infraDeps.LiveTailHub,
			AllowedOrigins: splitAllowedOrigins(appConfig.Server.AllowedOrigins),
			Sessions:       infraDeps.SessionManager,
		}),
		metrics.NewModule(nativeQuerier, getTenant),
		otlp_streamworkers.NewModule(infraDeps.CH, infraDeps.OTLP.LogDispatcher, infraDeps.OTLP.SpanDispatcher, infraDeps.OTLP.MetricDispatcher, infraDeps.LiveTailHub, appConfig.IngestionBatchMaxRows(), appConfig.IngestionBatchMaxWait()),
		otlp_spans.NewModule(infraDeps.OTLP.Authenticator, infraDeps.OTLP.Tracker, infraDeps.OTLP.SpanDispatcher),
		otlp_logs.NewModule(infraDeps.OTLP.Authenticator, infraDeps.OTLP.Tracker, infraDeps.OTLP.LogDispatcher),
		otlp_metrics.NewModule(infraDeps.OTLP.Authenticator, infraDeps.OTLP.Tracker, infraDeps.OTLP.MetricDispatcher),
		overview_errors.NewModule(nativeQuerier, getTenant),
		overview_overview.NewModule(nativeQuerier, getTenant),
		overview_redmetrics.NewModule(nativeQuerier, getTenant),
		overview_slo.NewModule(nativeQuerier, getTenant),
		saturation_explorer.NewModule(nativeQuerier, getTenant),
		saturation_database_collection.NewModule(nativeQuerier, getTenant),
		saturation_database_connections.NewModule(nativeQuerier, getTenant),
		saturation_database_errors.NewModule(nativeQuerier, getTenant),
		saturation_database_latency.NewModule(nativeQuerier, getTenant),
		saturation_database_slowqueries.NewModule(nativeQuerier, getTenant),
		saturation_database_summary.NewModule(nativeQuerier, getTenant),
		saturation_database_system.NewModule(nativeQuerier, getTenant),
		saturation_database_systems.NewModule(nativeQuerier, getTenant),
		saturation_database_volume.NewModule(nativeQuerier, getTenant),
		saturation_kafka.NewModule(nativeQuerier, getTenant),
		services_topology.NewModule(nativeQuerier, getTenant),
		spans_explorer.NewModule(nativeQuerier, getTenant),
		spans_livetail.NewModule(nativeQuerier, getTenant, nil),
		spans_tracedetail.NewModule(nativeQuerier, getTenant),
		spans_traces.NewModule(nativeQuerier, getTenant),
		user_auth.NewModule(infraDeps.DB, getTenant, infraDeps.SessionManager, appConfig),
		user_team.NewModule(infraDeps.DB, getTenant, appConfig),
		user_user.NewModule(infraDeps.DB, getTenant, appConfig),
	}
}

func splitAllowedOrigins(allowed string) []string {
	var out []string
	for _, o := range strings.Split(allowed, ",") {
		o = strings.TrimSpace(o)
		if o != "" {
			out = append(out, o)
		}
	}
	return out
}
