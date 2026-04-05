package dashboard

import (
	"database/sql"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

// Repository encapsulates data access for saved page overrides.
type Repository interface {
	GetPageOverride(teamID int64, pageID string) (pageOverrideDTO, error)
	SavePageOverride(teamID int64, pageID, configJSON string) error
}

// MySQLRepository stores page overrides in the teams.dashboard_configs JSON column.
type MySQLRepository struct {
	db dbutil.Querier
}

func NewRepository(db *sql.DB, appConfig registry.AppConfig) *MySQLRepository {
	return &MySQLRepository{
		db: dbutil.NewMySQLWrapper(db),
	}
}

// GetPageOverride returns the saved override JSON for a page from the teams table. Empty string means none.
func (r *MySQLRepository) GetPageOverride(teamID int64, pageID string) (pageOverrideDTO, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT JSON_UNQUOTE(JSON_EXTRACT(dashboard_configs, CONCAT('$.', ?))) AS config_json
		FROM teams WHERE id = ?
	`, pageID, teamID)
	if err != nil {
		return pageOverrideDTO{}, err
	}
	if value, ok := row["config_json"].(string); ok && value != "null" {
		return pageOverrideDTO{ConfigJSON: value}, nil
	}
	return pageOverrideDTO{}, nil
}

// SavePageOverride inserts or updates a page override JSON blob in the teams table.
func (r *MySQLRepository) SavePageOverride(teamID int64, pageID, configJSON string) error {
	_, err := r.db.Exec(`
		UPDATE teams
		SET dashboard_configs = JSON_SET(COALESCE(dashboard_configs, '{}'), CONCAT('$.', ?), CAST(? AS JSON))
		WHERE id = ?
	`, pageID, configJSON, teamID)
	return err
}
