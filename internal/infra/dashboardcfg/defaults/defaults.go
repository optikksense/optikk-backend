package defaults

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"
	"github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg/defaults/overview"
)

// Load creates a Registry populated with the built-in default page configurations.
func Load() (*dashboardcfg.Registry, error) {
	return dashboardcfg.LoadFromDocuments(defaultPageDocuments())
}

func defaultPageDocuments() []dashboardcfg.PageDocument {
	return []dashboardcfg.PageDocument{
		overview.Page(),
		// infrastructure: UI is optikk-frontend InfrastructureHubPage (no embedded default page).
	}
}
