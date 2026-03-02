package common

import (
	"database/sql"
)

// DBTenant holds dependencies shared by most HTTP modules.
type DBTenant struct {
	DB        *sql.DB
	GetTenant GetTenantFunc
}
