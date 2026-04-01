package server

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	otlp_logs "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/logs"
	otlp_metrics "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/metrics"
	otlp_spans "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/spans"
	ai_conversations "github.com/Optikk-Org/optikk-backend/internal/modules/ai/conversations"
	ai_dashboard "github.com/Optikk-Org/optikk-backend/internal/modules/ai/dashboard"
	ai_rundetail "github.com/Optikk-Org/optikk-backend/internal/modules/ai/rundetail"
	ai_runs "github.com/Optikk-Org/optikk-backend/internal/modules/ai/runs"
	ai_traces "github.com/Optikk-Org/optikk-backend/internal/modules/ai/traces"
	"github.com/Optikk-Org/optikk-backend/internal/modules/apm"
	explorer_analytics "github.com/Optikk-Org/optikk-backend/internal/modules/explorer/analytics"
	defaultconfig "github.com/Optikk-Org/optikk-backend/internal/modules/dashboard"
	"github.com/Optikk-Org/optikk-backend/internal/modules/httpmetrics"
	infrastructure_cpu "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/cpu"
	infrastructure_disk "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/disk"
	infrastructure_jvm "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/jvm"
	infrastructure_kubernetes "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/kubernetes"
	infrastructure_memory "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/memory"
	infrastructure_network "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/network"
	infrastructure_nodes "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/nodes"
	infrastructure_resource_utilisation "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/resourceutil"
	log_detail "github.com/Optikk-Org/optikk-backend/internal/modules/logs/detail"
	log_explorer "github.com/Optikk-Org/optikk-backend/internal/modules/logs/explorer"
	log_patterns "github.com/Optikk-Org/optikk-backend/internal/modules/logs/patterns"
	log_search "github.com/Optikk-Org/optikk-backend/internal/modules/logs/search"
	log_transactions "github.com/Optikk-Org/optikk-backend/internal/modules/logs/transactions"
	log_tracelogs "github.com/Optikk-Org/optikk-backend/internal/modules/logs/tracelogs"
	overview_errors "github.com/Optikk-Org/optikk-backend/internal/modules/overview/errors"
	overview_overview "github.com/Optikk-Org/optikk-backend/internal/modules/overview/overview"
	overview_slo "github.com/Optikk-Org/optikk-backend/internal/modules/overview/slo"
	saturation_database_collection "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/collection"
	saturation_database_connections "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/connections"
	saturation_database_errors "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/errors"
	saturation_database_latency "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/latency"
	saturation_database_slowqueries "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/slowqueries"
	saturation_database_summary "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/summary"
	saturation_database_system "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/system"
	saturation_database_systems "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/systems"
	saturation_database_volume "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/volume"
	saturation_kafka "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka"
	saturation_redis "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/redis"
	services_service "github.com/Optikk-Org/optikk-backend/internal/modules/services/service"
	services_servicemap "github.com/Optikk-Org/optikk-backend/internal/modules/services/servicemap"
	spans_errorfingerprint "github.com/Optikk-Org/optikk-backend/internal/modules/traces/errorfingerprint"
	spans_errortracking "github.com/Optikk-Org/optikk-backend/internal/modules/traces/errortracking"
	spans_explorer "github.com/Optikk-Org/optikk-backend/internal/modules/traces/explorer"
	spans_livetail "github.com/Optikk-Org/optikk-backend/internal/modules/traces/livetail"
	spans_traces "github.com/Optikk-Org/optikk-backend/internal/modules/traces/query"
	spans_redmetrics "github.com/Optikk-Org/optikk-backend/internal/modules/traces/redmetrics"
	spans_tracecompare "github.com/Optikk-Org/optikk-backend/internal/modules/traces/tracecompare"
	spans_tracedetail "github.com/Optikk-Org/optikk-backend/internal/modules/traces/tracedetail"
	user_auth "github.com/Optikk-Org/optikk-backend/internal/modules/user/auth"
	user_team "github.com/Optikk-Org/optikk-backend/internal/modules/user/team"
	user_user "github.com/Optikk-Org/optikk-backend/internal/modules/user/user"
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
		explorer_analytics.NewModule(nativeQuerier, getTenant),
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
		log_detail.NewModule(nativeQuerier, getTenant),
		log_explorer.NewModule(nativeQuerier, getTenant),
		log_patterns.NewModule(nativeQuerier, getTenant),
		log_search.NewModule(nativeQuerier, getTenant),
		log_transactions.NewModule(nativeQuerier, getTenant),
		log_tracelogs.NewModule(nativeQuerier, getTenant),
		otlp_spans.NewModule(sqlDB, clickHouseConn, appConfig),
		otlp_logs.NewModule(sqlDB, clickHouseConn, appConfig),
		otlp_metrics.NewModule(sqlDB, clickHouseConn, appConfig),
		overview_errors.NewModule(nativeQuerier, getTenant),
		overview_overview.NewModule(nativeQuerier, getTenant),
		overview_slo.NewModule(nativeQuerier, getTenant),
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
		saturation_redis.NewModule(nativeQuerier, getTenant),
		services_service.NewModule(nativeQuerier, getTenant),
		services_servicemap.NewModule(nativeQuerier, getTenant),
		spans_errorfingerprint.NewModule(nativeQuerier, getTenant),
		spans_errortracking.NewModule(nativeQuerier, getTenant),
		spans_explorer.NewModule(nativeQuerier, getTenant),
		spans_livetail.NewModule(nativeQuerier, getTenant),
		spans_redmetrics.NewModule(nativeQuerier, getTenant),
		spans_tracecompare.NewModule(nativeQuerier, getTenant),
		spans_tracedetail.NewModule(nativeQuerier, getTenant),
		spans_traces.NewModule(nativeQuerier, getTenant),
		user_auth.NewModule(sqlDB, getTenant, sessionManager, appConfig),
		user_team.NewModule(sqlDB, getTenant, configRegistry, appConfig),
		user_user.NewModule(sqlDB, getTenant, appConfig),
	}
}
