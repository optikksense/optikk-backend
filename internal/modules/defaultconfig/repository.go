package defaultconfig

import (
	"database/sql"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// Repository encapsulates data access for saved page overrides.
type Repository interface {
	EnsureTable() error
	GetPageOverride(teamID int64, pageID string) (string, error)
	SavePageOverride(teamID int64, pageID, configJSON string) error
}

// MySQLRepository stores page overrides in MySQL.
type MySQLRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new default-config repository.
func NewRepository(db *sql.DB) *MySQLRepository {
	return &MySQLRepository{db: dbutil.NewMySQLWrapper(db)}
}

// EnsureTable creates the page override table when needed.
func (r *MySQLRepository) EnsureTable() error {
	_, err := r.db.Exec(`
		CREATE TABLE IF NOT EXISTS default_page_configs (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			team_id BIGINT NOT NULL,
			page_id VARCHAR(100) NOT NULL,
			config_json LONGTEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uq_team_page (team_id, page_id)
		)
	`)
	return err
}

// GetPageOverride returns the saved override JSON for a page. Empty string means none.
func (r *MySQLRepository) GetPageOverride(teamID int64, pageID string) (string, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT config_json FROM default_page_configs
		WHERE team_id = ? AND page_id = ?
	`, teamID, pageID)
	if err != nil {
		return "", err
	}
	if value, ok := row["config_json"].(string); ok {
		return value, nil
	}
	return "", nil
}

// SavePageOverride inserts or updates a page override JSON blob.
func (r *MySQLRepository) SavePageOverride(teamID int64, pageID, configJSON string) error {
	_, err := r.db.Exec(`
		INSERT INTO default_page_configs (team_id, page_id, config_json)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE config_json = VALUES(config_json), updated_at = CURRENT_TIMESTAMP
	`, teamID, pageID, configJSON)
	return err
}
