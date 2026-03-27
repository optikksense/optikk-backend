package httputil

import (
	"database/sql"
)

type DBTenant struct {
	DB        *sql.DB
	GetTenant GetTenantFunc
}
