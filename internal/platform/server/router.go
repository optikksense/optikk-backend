package server

import (
	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/ai"
	"github.com/observability/observability-backend-go/internal/modules/dashboardconfig"
	nodes "github.com/observability/observability-backend-go/internal/modules/infrastructure/nodes"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/resource_utilisation"
	logsmodule "github.com/observability/observability-backend-go/internal/modules/log"
	overviewerrors "github.com/observability/observability-backend-go/internal/modules/overview/errors"
	overviewmodule "github.com/observability/observability-backend-go/internal/modules/overview/overview"
	overviewslo "github.com/observability/observability-backend-go/internal/modules/overview/slo"
	"github.com/observability/observability-backend-go/internal/modules/saturation/database"
	"github.com/observability/observability-backend-go/internal/modules/saturation/kafka"
	servicepage "github.com/observability/observability-backend-go/internal/modules/services/service"
	servicetopology "github.com/observability/observability-backend-go/internal/modules/services/topology"
	traces "github.com/observability/observability-backend-go/internal/modules/spans"
	identity "github.com/observability/observability-backend-go/internal/modules/user"
)

type moduleConfigs struct {
	Identity            identity.Config
	Overview            overviewmodule.Config
	OverviewSLO         overviewslo.Config
	OverviewErrors      overviewerrors.Config
	ServicesPage        servicepage.Config
	ServicesTopology    servicetopology.Config
	Nodes               nodes.Config
	ResourceUtilisation resource_utilisation.Config
	SaturationDatabase  database.Config
	SaturationKafka     kafka.Config
	Logs                logsmodule.Config
	Traces              traces.Config
	AI                  ai.Config
	DashboardConfig     dashboardconfig.Config
}

func defaultModuleConfigs() moduleConfigs {
	return moduleConfigs{
		Identity:            identity.DefaultConfig(),
		Overview:            overviewmodule.DefaultConfig(),
		OverviewSLO:         overviewslo.DefaultConfig(),
		OverviewErrors:      overviewerrors.DefaultConfig(),
		ServicesPage:        servicepage.DefaultConfig(),
		ServicesTopology:    servicetopology.DefaultConfig(),
		Nodes:               nodes.DefaultConfig(),
		ResourceUtilisation: resource_utilisation.DefaultConfig(),
		SaturationDatabase:  database.DefaultConfig(),
		SaturationKafka:     kafka.DefaultConfig(),
		Logs:                logsmodule.DefaultConfig(),
		Traces:              traces.DefaultConfig(),
		AI:                  ai.DefaultConfig(),
		DashboardConfig:     dashboardconfig.DefaultConfig(),
	}
}

func (a *App) registerRoutes(r *gin.Engine) {
	cfg := defaultModuleConfigs()

	api := r.Group("/api")
	v1 := r.Group("/api/v1")

	identity.RegisterRoutes(cfg.Identity, api, v1, a.Auth, a.Users)
	overviewmodule.RegisterRoutes(cfg.Overview, api, v1, a.Overview)
	overviewslo.RegisterRoutes(cfg.OverviewSLO, api, v1, a.OverviewSLO)
	overviewerrors.RegisterRoutes(cfg.OverviewErrors, api, v1, a.OverviewErrors)
	servicepage.RegisterRoutes(cfg.ServicesPage, api, v1, a.ServicesPage)
	servicetopology.RegisterRoutes(cfg.ServicesTopology, api, v1, a.ServicesTopology)
	nodes.RegisterRoutes(cfg.Nodes, api, v1, a.Nodes)
	resource_utilisation.RegisterRoutes(cfg.ResourceUtilisation, api, v1, a.ResourceUtilisation)
	database.RegisterRoutes(cfg.SaturationDatabase, api, v1, a.SaturationDatabase)
	kafka.RegisterRoutes(cfg.SaturationKafka, api, v1, a.SaturationKafka)
	logsmodule.RegisterRoutes(cfg.Logs, api, v1, a.Logs)
	traces.RegisterRoutes(cfg.Traces, api, v1, a.Traces)
	ai.RegisterRoutes(cfg.AI, api, v1, a.AI)
	dashboardconfig.RegisterRoutes(cfg.DashboardConfig, api, v1, a.DashboardConfig)
}
