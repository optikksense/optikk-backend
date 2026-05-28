package server

import (
	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"

	infrastructure_cpu "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/cpu"
	infrastructure_disk "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/disk"
	infrastructure_fleet "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/fleet"
	infrastructure_memory "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/memory"
	infrastructure_network "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/network"
	infrastructure_nodes "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/nodes"
	log_explorer "github.com/Optikk-Org/optikk-backend/internal/modules/logs/explorer"
	log_facets "github.com/Optikk-Org/optikk-backend/internal/modules/logs/facets"
	log_detail "github.com/Optikk-Org/optikk-backend/internal/modules/logs/logdetail"
	log_trace_logs "github.com/Optikk-Org/optikk-backend/internal/modules/logs/trace_logs"
	log_trends "github.com/Optikk-Org/optikk-backend/internal/modules/logs/trends"
	metrics_explorer "github.com/Optikk-Org/optikk-backend/internal/modules/metrics/explorer"
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
	saturation_kafka_client "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka/client"
	saturation_kafka_consumer "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka/consumer"
	saturation_kafka_explorer "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka/explorer"
	saturation_kafka_producer "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka/producer"
	"github.com/Optikk-Org/optikk-backend/internal/modules/services/deployments"
	services_errors "github.com/Optikk-Org/optikk-backend/internal/modules/services/errors"
	services_hosts "github.com/Optikk-Org/optikk-backend/internal/modules/services/hosts"
	services_latency "github.com/Optikk-Org/optikk-backend/internal/modules/services/latency"
	services_redmetrics "github.com/Optikk-Org/optikk-backend/internal/modules/services/redmetrics"
	services_topology "github.com/Optikk-Org/optikk-backend/internal/modules/services/topology"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/detail"
	spans_explorer "github.com/Optikk-Org/optikk-backend/internal/modules/traces/explorer"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/facets"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/paths"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/servicemap"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/shape"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/span_query"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/suggest"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/trend"
	alerting_evaluator "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/evaluator"
	alerting_monitors "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/monitors"
	alerting_notifications "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/notifications"
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
		deployments.NewModule(nativeQuerier, getTenant),

		infrastructure_cpu.NewModule(nativeQuerier, getTenant),
		infrastructure_disk.NewModule(nativeQuerier, getTenant),
		infrastructure_memory.NewModule(nativeQuerier, getTenant),
		infrastructure_network.NewModule(nativeQuerier, getTenant),
		infrastructure_fleet.NewModule(nativeQuerier, getTenant),
		infrastructure_nodes.NewModule(nativeQuerier, getTenant),
		log_explorer.NewModule(nativeQuerier, getTenant),
		log_detail.NewModule(nativeQuerier, getTenant),
		log_facets.NewModule(nativeQuerier, getTenant),
		log_trends.NewModule(nativeQuerier, getTenant),
		log_trace_logs.NewModule(nativeQuerier, getTenant),
		metrics_explorer.NewModule(nativeQuerier, getTenant),
		infraDeps.Ingest.Logs,
		infraDeps.Ingest.Metrics,
		infraDeps.Ingest.Spans,
		services_errors.NewModule(nativeQuerier, getTenant, infraDeps.RedisClient),
		services_hosts.NewModule(nativeQuerier, getTenant),
		services_redmetrics.NewModule(nativeQuerier, getTenant),
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
		saturation_kafka_producer.NewModule(nativeQuerier, getTenant),
		saturation_kafka_consumer.NewModule(nativeQuerier, getTenant),
		saturation_kafka_client.NewModule(nativeQuerier, getTenant),
		saturation_kafka_explorer.NewModule(nativeQuerier, getTenant),
		services_topology.NewModule(nativeQuerier, getTenant),
		spans_explorer.NewModule(nativeQuerier, getTenant),
		detail.NewModule(nativeQuerier, getTenant),
		span_query.NewModule(nativeQuerier, getTenant),
		facets.NewModule(nativeQuerier, getTenant),
		paths.NewModule(nativeQuerier, getTenant),
		servicemap.NewModule(nativeQuerier, getTenant),
		shape.NewModule(nativeQuerier, getTenant),
		suggest.NewModule(nativeQuerier, getTenant),
		trend.NewModule(nativeQuerier, getTenant),
		services_latency.NewModule(nativeQuerier, getTenant),
		user_auth.NewModule(infraDeps.DB, getTenant, infraDeps.SessionManager, appConfig),
		user_team.NewModule(infraDeps.DB, getTenant, appConfig),
		user_user.NewModule(infraDeps.DB, getTenant, appConfig),

		alerting_monitors.NewModule(infraDeps.DB, getTenant, nativeQuerier),
		alerting_notifications.NewModule(infraDeps.DB, getTenant),
		alerting_evaluator.NewModule(infraDeps.DB, nativeQuerier),
	}
}
