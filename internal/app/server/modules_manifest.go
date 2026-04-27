package server

import (
	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"

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
	log_facets "github.com/Optikk-Org/optikk-backend/internal/modules/logs/log_facets"
	log_trends "github.com/Optikk-Org/optikk-backend/internal/modules/logs/log_trends"
	log_explorer "github.com/Optikk-Org/optikk-backend/internal/modules/logs/explorer"
	log_detail "github.com/Optikk-Org/optikk-backend/internal/modules/logs/logdetail"
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
	traces_errors "github.com/Optikk-Org/optikk-backend/internal/modules/traces/errors"
	spans_explorer "github.com/Optikk-Org/optikk-backend/internal/modules/traces/explorer"
	traces_latency "github.com/Optikk-Org/optikk-backend/internal/modules/traces/latency"
	span_query "github.com/Optikk-Org/optikk-backend/internal/modules/traces/span_query"
	trace_paths "github.com/Optikk-Org/optikk-backend/internal/modules/traces/trace_paths"
	trace_servicemap "github.com/Optikk-Org/optikk-backend/internal/modules/traces/trace_servicemap"
	trace_shape "github.com/Optikk-Org/optikk-backend/internal/modules/traces/trace_shape"
	trace_suggest "github.com/Optikk-Org/optikk-backend/internal/modules/traces/trace_suggest"
	spans_tracedetail "github.com/Optikk-Org/optikk-backend/internal/modules/traces/tracedetail"
	user_auth "github.com/Optikk-Org/optikk-backend/internal/modules/user/auth"
	user_team "github.com/Optikk-Org/optikk-backend/internal/modules/user/team"
	user_user "github.com/Optikk-Org/optikk-backend/internal/modules/user/user"
)

func configuredModules(
	nativeQuerier clickhouse.Conn,
	getTenant registry.GetTenantFunc,
	appConfig registry.AppConfig,
	infraDeps *Infra,
) []registry.Module {
	return []registry.Module{
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
		log_detail.NewModule(nativeQuerier, getTenant),
		log_facets.NewModule(nativeQuerier, getTenant),
		log_trends.NewModule(nativeQuerier, getTenant),
		metrics.NewModule(nativeQuerier, getTenant),
		infraDeps.Ingest.Logs,
		infraDeps.Ingest.Metrics,
		infraDeps.Ingest.Spans,
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
		spans_tracedetail.NewModule(nativeQuerier, getTenant),
		span_query.NewModule(nativeQuerier, getTenant),
		trace_paths.NewModule(nativeQuerier, getTenant),
		trace_servicemap.NewModule(nativeQuerier, getTenant),
		trace_shape.NewModule(nativeQuerier, getTenant),
		trace_suggest.NewModule(nativeQuerier, getTenant),
		traces_errors.NewModule(nativeQuerier, getTenant),
		traces_latency.NewModule(nativeQuerier, getTenant),
		user_auth.NewModule(infraDeps.DB, getTenant, infraDeps.SessionManager, appConfig),
		user_team.NewModule(infraDeps.DB, getTenant, appConfig),
		user_user.NewModule(infraDeps.DB, getTenant, appConfig),
	}
}
