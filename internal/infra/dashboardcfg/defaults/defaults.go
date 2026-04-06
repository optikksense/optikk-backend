package defaults

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"
	"github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg/defaults/ai_observability"
	"github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg/defaults/infrastructure"
	"github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg/defaults/overview"
	"github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg/defaults/saturation"
	"github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg/defaults/service"
)

// Load creates a Registry populated with the built-in default page configurations.
func Load() (*dashboardcfg.Registry, error) {
	return dashboardcfg.LoadFromDocuments(defaultPageDocuments())
}

func defaultPageDocuments() []dashboardcfg.PageDocument {
	return []dashboardcfg.PageDocument{
		overview.Page(),
		saturation.Page(),
		infrastructure.Page(),
		ai_observability.Page(),
		service.Page(),
	}
}
