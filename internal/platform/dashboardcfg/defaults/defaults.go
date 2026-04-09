package defaults

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/platform/dashboardcfg"
	"github.com/Optikk-Org/optikk-backend/internal/platform/dashboardcfg/defaults/ai_observability"
	"github.com/Optikk-Org/optikk-backend/internal/platform/dashboardcfg/defaults/infrastructure"
	"github.com/Optikk-Org/optikk-backend/internal/platform/dashboardcfg/defaults/overview"
)

// Load creates a Registry populated with the built-in default page configurations.
func Load() (*dashboardcfg.Registry, error) {
	return dashboardcfg.LoadFromDocuments(defaultPageDocuments())
}

func defaultPageDocuments() []dashboardcfg.PageDocument {
	return []dashboardcfg.PageDocument{
		overview.Page(),
		infrastructure.Page(),
		ai_observability.Page(),
	}
}
