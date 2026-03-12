package common

import (
	"database/sql"
)

type DBTenant struct {
	DB        *sql.DB
	GetTenant GetTenantFunc
}
