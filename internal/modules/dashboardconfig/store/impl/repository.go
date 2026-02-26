package impl

import (
	"database/sql"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// MySQLRepository encapsulates data access for dashboard chart configurations.
type MySQLRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new DashboardConfig Repository.
func NewRepository(db *sql.DB) *MySQLRepository {
	return &MySQLRepository{db: dbutil.NewMySQLWrapper(db)}
}

// EnsureTable creates the dashboard_chart_configs table if it does not exist.
func (r *MySQLRepository) EnsureTable() error {
	_, err := r.db.Exec(`
		CREATE TABLE IF NOT EXISTS dashboard_chart_configs (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			team_id BIGINT NOT NULL,
			page_id VARCHAR(100) NOT NULL,
			config_yaml TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uq_team_page (team_id, page_id)
		)
	`)
	return err
}

// GetConfig returns the YAML config for a team+page. Returns empty string if not found.
func (r *MySQLRepository) GetConfig(teamID int64, pageID string) (string, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT config_yaml FROM dashboard_chart_configs
		WHERE team_id = ? AND page_id = ?
	`, teamID, pageID)
	if err != nil {
		return "", err
	}
	if yaml, ok := row["config_yaml"]; ok {
		if s, ok := yaml.(string); ok {
			return s, nil
		}
	}
	return "", nil
}

// SaveConfig inserts or updates the YAML config for a team+page.
func (r *MySQLRepository) SaveConfig(teamID int64, pageID, configYaml string) error {
	_, err := r.db.Exec(`
		INSERT INTO dashboard_chart_configs (team_id, page_id, config_yaml)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE config_yaml = VALUES(config_yaml), updated_at = CURRENT_TIMESTAMP
	`, teamID, pageID, configYaml)
	return err
}
