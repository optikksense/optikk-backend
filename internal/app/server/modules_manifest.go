package server

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	otlp_logs "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/logs"
	otlp_metrics "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/metrics"
	otlp_spans "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/spans"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting"

	infrastructure_connpool "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/connpool"
	infrastructure_cpu "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/cpu"
	infrastructure_disk "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/disk"
	infrastructure_fleet "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/fleet"
	infrastructure_jvm "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/jvm"
	infrastructure_kubernetes "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/kubernetes"
	infrastructure_memory "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/memory"
	infrastructure_network "github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/network"
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
	saturation_database_latency "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/latency"
	saturation_database_slowqueries "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/slowqueries"
	saturation_database_summary "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/summary"
	saturation_database_system "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/system"
	saturation_database_systems "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/systems"
	saturation_database_volume "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/volume"
	saturation_explorer "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/explorer"
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

// allModules is the single module registration table.
// To add a new module: import it and append one line here.
var allModules = []registry.NewModuleFunc{
	ai_explorer.NewModule,
	alerting.NewModule,
	apm.NewModule,
	deployments.NewModule,
	httpmetrics.NewModule,
	infrastructure_connpool.NewModule,
	infrastructure_cpu.NewModule,
	infrastructure_disk.NewModule,
	infrastructure_fleet.NewModule,
	infrastructure_jvm.NewModule,
	infrastructure_kubernetes.NewModule,
	infrastructure_memory.NewModule,
	infrastructure_network.NewModule,
	infrastructure_nodes.NewModule,
	infrastructure_resource_utilisation.NewModule,
	livetail.NewModule,
	llm_hub.NewModule,
	log_explorer.NewModule,
	log_search.NewModule,
	metrics.NewModule,
	otlp_logs.NewModule,
	otlp_metrics.NewModule,
	otlp_spans.NewModule,
	overview_errors.NewModule,
	overview_overview.NewModule,
	overview_redmetrics.NewModule,
	overview_slo.NewModule,
	saturation_database_collection.NewModule,
	saturation_database_connections.NewModule,
	saturation_database_errors.NewModule,
	saturation_database_latency.NewModule,
	saturation_database_slowqueries.NewModule,
	saturation_database_summary.NewModule,
	saturation_database_system.NewModule,
	saturation_database_systems.NewModule,
	saturation_database_volume.NewModule,
	saturation_explorer.NewModule,
	saturation_kafka.NewModule,
	services_topology.NewModule,
	spans_explorer.NewModule,
	spans_livetail.NewModule,
	spans_tracedetail.NewModule,
	spans_traces.NewModule,
	user_auth.NewModule,
	user_team.NewModule,
	user_user.NewModule,
}

func configuredModules(deps *registry.Deps) ([]registry.Module, error) {
	modules := make([]registry.Module, 0, len(allModules))
	for _, fn := range allModules {
		mod, err := fn(deps)
		if err != nil {
			return nil, err
		}
		modules = append(modules, mod)
	}
	return modules, nil
}
