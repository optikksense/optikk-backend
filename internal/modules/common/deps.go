package common

import (
	"database/sql"

	apphandlers "github.com/observability/observability-backend-go/internal/platform/handlers"
)

// DBTenant holds dependencies shared by most HTTP modules.
type DBTenant struct {
	DB        *sql.DB
	GetTenant apphandlers.GetTenantFunc
}
