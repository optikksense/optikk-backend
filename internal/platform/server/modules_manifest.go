package server

import (
	ai_conversations "github.com/observability/observability-backend-go/internal/modules/ai/conversations"
	ai_dashboard "github.com/observability/observability-backend-go/internal/modules/ai/dashboard"
	ai_rundetail "github.com/observability/observability-backend-go/internal/modules/ai/rundetail"
	ai_runs "github.com/observability/observability-backend-go/internal/modules/ai/runs"
	ai_traces "github.com/observability/observability-backend-go/internal/modules/ai/traces"
	"github.com/observability/observability-backend-go/internal/modules/apm"
	database_collection "github.com/observability/observability-backend-go/internal/modules/database/collection"
	database_connections "github.com/observability/observability-backend-go/internal/modules/database/connections"
	database_errors "github.com/observability/observability-backend-go/internal/modules/database/errors"
	database_latency "github.com/observability/observability-backend-go/internal/modules/database/latency"
	database_slowqueries "github.com/observability/observability-backend-go/internal/modules/database/slowqueries"
	database_summary "github.com/observability/observability-backend-go/internal/modules/database/summary"
	database_system "github.com/observability/observability-backend-go/internal/modules/database/system"
	database_systems "github.com/observability/observability-backend-go/internal/modules/database/systems"
	database_volume "github.com/observability/observability-backend-go/internal/modules/database/volume"
	defaultconfig "github.com/observability/observability-backend-go/internal/modules/defaultconfig"
	"github.com/observability/observability-backend-go/internal/modules/httpmetrics"
	infrastructure_cpu "github.com/observability/observability-backend-go/internal/modules/infrastructure/cpu"
	infrastructure_disk "github.com/observability/observability-backend-go/internal/modules/infrastructure/disk"
	infrastructure_jvm "github.com/observability/observability-backend-go/internal/modules/infrastructure/jvm"
	infrastructure_kubernetes "github.com/observability/observability-backend-go/internal/modules/infrastructure/kubernetes"
	infrastructure_memory "github.com/observability/observability-backend-go/internal/modules/infrastructure/memory"
	infrastructure_network "github.com/observability/observability-backend-go/internal/modules/infrastructure/network"
	infrastructure_nodes "github.com/observability/observability-backend-go/internal/modules/infrastructure/nodes"
	infrastructure_resource_utilisation "github.com/observability/observability-backend-go/internal/modules/infrastructure/resource_utilisation"
	log_analytics "github.com/observability/observability-backend-go/internal/modules/log/analytics"
	log_detail "github.com/observability/observability-backend-go/internal/modules/log/detail"
	log_explorer "github.com/observability/observability-backend-go/internal/modules/log/explorer"
	log_search "github.com/observability/observability-backend-go/internal/modules/log/search"
	log_tracelogs "github.com/observability/observability-backend-go/internal/modules/log/tracelogs"
	"github.com/observability/observability-backend-go/internal/modules/otlp"
	overview_errors "github.com/observability/observability-backend-go/internal/modules/overview/errors"
	overview_overview "github.com/observability/observability-backend-go/internal/modules/overview/overview"
	overview_slo "github.com/observability/observability-backend-go/internal/modules/overview/slo"
	"github.com/observability/observability-backend-go/internal/modules/registry"
	saturation_kafka "github.com/observability/observability-backend-go/internal/modules/saturation/kafka"
	saturation_redis "github.com/observability/observability-backend-go/internal/modules/saturation/redis"
	services_service "github.com/observability/observability-backend-go/internal/modules/services/service"
	services_servicemap "github.com/observability/observability-backend-go/internal/modules/services/servicemap"
	services_topology "github.com/observability/observability-backend-go/internal/modules/services/topology"
	spans_analytics "github.com/observability/observability-backend-go/internal/modules/spans/analytics"
	spans_errorfingerprint "github.com/observability/observability-backend-go/internal/modules/spans/errorfingerprint"
	spans_errortracking "github.com/observability/observability-backend-go/internal/modules/spans/errortracking"
	spans_explorer "github.com/observability/observability-backend-go/internal/modules/spans/explorer"
	spans_livetail "github.com/observability/observability-backend-go/internal/modules/spans/livetail"
	spans_redmetrics "github.com/observability/observability-backend-go/internal/modules/spans/redmetrics"
	spans_tracecompare "github.com/observability/observability-backend-go/internal/modules/spans/tracecompare"
	spans_tracedetail "github.com/observability/observability-backend-go/internal/modules/spans/tracedetail"
	spans_traces "github.com/observability/observability-backend-go/internal/modules/spans/traces"
	user_auth "github.com/observability/observability-backend-go/internal/modules/user/auth"
	user_team "github.com/observability/observability-backend-go/internal/modules/user/team"
	user_user "github.com/observability/observability-backend-go/internal/modules/user/user"
)

func configuredModules(
	nativeQuerier *registry.NativeQuerier,
	sqlDB *registry.SQLDB,
	clickHouseConn registry.ClickHouseConn,
	getTenant registry.GetTenantFunc,
	sessionManager *registry.SessionManager,
	appConfig registry.AppConfig,
	configRegistry *registry.ConfigRegistry,
) []registry.Module {
	return []registry.Module{
		ai_conversations.NewModule(nativeQuerier, getTenant),
		ai_dashboard.NewModule(nativeQuerier, getTenant),
		ai_rundetail.NewModule(nativeQuerier, getTenant),
		ai_runs.NewModule(nativeQuerier, getTenant),
		ai_traces.NewModule(nativeQuerier, getTenant),
		apm.NewModule(nativeQuerier, getTenant),
		database_collection.NewModule(nativeQuerier, getTenant),
		database_connections.NewModule(nativeQuerier, getTenant),
		database_errors.NewModule(nativeQuerier, getTenant),
		database_latency.NewModule(nativeQuerier, getTenant),
		database_slowqueries.NewModule(nativeQuerier, getTenant),
		database_summary.NewModule(nativeQuerier, getTenant),
		database_system.NewModule(nativeQuerier, getTenant),
		database_systems.NewModule(nativeQuerier, getTenant),
		database_volume.NewModule(nativeQuerier, getTenant),
		defaultconfig.NewModule(sqlDB, getTenant, appConfig, configRegistry),
		httpmetrics.NewModule(nativeQuerier, getTenant),
		infrastructure_cpu.NewModule(nativeQuerier, getTenant),
		infrastructure_disk.NewModule(nativeQuerier, getTenant),
		infrastructure_jvm.NewModule(nativeQuerier, getTenant),
		infrastructure_kubernetes.NewModule(nativeQuerier, getTenant),
		infrastructure_memory.NewModule(nativeQuerier, getTenant),
		infrastructure_network.NewModule(nativeQuerier, getTenant),
		infrastructure_nodes.NewModule(nativeQuerier, getTenant),
		infrastructure_resource_utilisation.NewModule(nativeQuerier, getTenant),
		log_analytics.NewModule(nativeQuerier, getTenant),
		log_detail.NewModule(nativeQuerier, getTenant),
		log_explorer.NewModule(nativeQuerier, getTenant),
		log_search.NewModule(nativeQuerier, getTenant),
		log_tracelogs.NewModule(nativeQuerier, getTenant),
		otlp.NewModule(sqlDB, clickHouseConn, appConfig),
		overview_errors.NewModule(nativeQuerier, getTenant),
		overview_overview.NewModule(nativeQuerier, getTenant),
		overview_slo.NewModule(nativeQuerier, getTenant),
		saturation_kafka.NewModule(nativeQuerier, getTenant),
		saturation_redis.NewModule(nativeQuerier, getTenant),
		services_service.NewModule(nativeQuerier, getTenant),
		services_servicemap.NewModule(nativeQuerier, getTenant),
		services_topology.NewModule(nativeQuerier, getTenant),
		spans_analytics.NewModule(nativeQuerier, getTenant),
		spans_errorfingerprint.NewModule(nativeQuerier, getTenant),
		spans_errortracking.NewModule(nativeQuerier, getTenant),
		spans_explorer.NewModule(nativeQuerier, getTenant),
		spans_livetail.NewModule(nativeQuerier, getTenant),
		spans_redmetrics.NewModule(nativeQuerier, getTenant),
		spans_tracecompare.NewModule(nativeQuerier, getTenant),
		spans_tracedetail.NewModule(nativeQuerier, getTenant),
		spans_traces.NewModule(nativeQuerier, getTenant),
		user_auth.NewModule(sqlDB, getTenant, sessionManager, appConfig),
		user_team.NewModule(sqlDB, getTenant, configRegistry),
		user_user.NewModule(sqlDB, getTenant),
	}
}
