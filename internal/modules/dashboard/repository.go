package dashboard

import (
	"database/sql"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
)

// Repository encapsulates data access for saved page overrides.
// Used as a dummy structure to adhere to the 6-file pattern after removing DB reliance.
type Repository interface{}

// MySQLRepository stores page overrides.
type MySQLRepository struct{}

func NewRepository(db *sql.DB, appConfig registry.AppConfig) *MySQLRepository {
	return &MySQLRepository{}
}
