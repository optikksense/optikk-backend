package dashboardconfig

import (
	"database/sql"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// VersionMeta holds version metadata without the full YAML body.
type VersionMeta struct {
	ID            int64  `json:"id"`
	Version       int    `json:"version"`
	ChangeSummary string `json:"changeSummary"`
	CreatedBy     string `json:"createdBy"`
	CreatedAt     string `json:"createdAt"`
}

// VersionRepository handles versioned dashboard config persistence.
type VersionRepository struct {
	db dbutil.Querier
}

// NewVersionRepository creates a new VersionRepository.
func NewVersionRepository(db *sql.DB) *VersionRepository {
	return &VersionRepository{db: dbutil.NewMySQLWrapper(db)}
}

// EnsureTable creates the dashboard_config_versions table if it does not exist.
func (r *VersionRepository) EnsureTable() error {
	_, err := r.db.Exec(`
		CREATE TABLE IF NOT EXISTS dashboard_config_versions (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			team_id BIGINT NOT NULL,
			page_id VARCHAR(100) NOT NULL,
			version INT NOT NULL,
			config_yaml TEXT NOT NULL,
			change_summary VARCHAR(500) DEFAULT '',
			created_by VARCHAR(255) DEFAULT '',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			INDEX idx_team_page_version (team_id, page_id, version)
		)
	`)
	return err
}

// SaveVersion inserts a new version snapshot.
// It auto-increments the version number for the given team+page.
func (r *VersionRepository) SaveVersion(teamID int64, pageID, configYaml, summary, createdBy string) (int, error) {
	// Get current max version
	row, err := dbutil.QueryMap(r.db, `
		SELECT COALESCE(MAX(version), 0) as max_version
		FROM dashboard_config_versions
		WHERE team_id = ? AND page_id = ?
	`, teamID, pageID)
	if err != nil {
		return 0, err
	}

	maxVersion := 0
	if v, ok := row["max_version"]; ok {
		switch val := v.(type) {
		case int64:
			maxVersion = int(val)
		case float64:
			maxVersion = int(val)
		}
	}

	nextVersion := maxVersion + 1

	_, err = r.db.Exec(`
		INSERT INTO dashboard_config_versions (team_id, page_id, version, config_yaml, change_summary, created_by)
		VALUES (?, ?, ?, ?, ?, ?)
	`, teamID, pageID, nextVersion, configYaml, summary, createdBy)
	if err != nil {
		return 0, err
	}

	return nextVersion, nil
}

// ListVersions returns version metadata for a given team+page, ordered by version DESC.
func (r *VersionRepository) ListVersions(teamID int64, pageID string, limit, offset int) ([]VersionMeta, error) {
	if limit <= 0 {
		limit = 20
	}

	rows, err := dbutil.QueryMaps(r.db, `
		SELECT id, version, change_summary, created_by, created_at
		FROM dashboard_config_versions
		WHERE team_id = ? AND page_id = ?
		ORDER BY version DESC
		LIMIT ? OFFSET ?
	`, teamID, pageID, limit, offset)
	if err != nil {
		return nil, err
	}

	var versions []VersionMeta
	for _, row := range rows {
		vm := VersionMeta{}
		if v, ok := row["id"].(int64); ok {
			vm.ID = v
		}
		if v, ok := row["version"]; ok {
			switch val := v.(type) {
			case int64:
				vm.Version = int(val)
			case float64:
				vm.Version = int(val)
			}
		}
		if v, ok := row["change_summary"].(string); ok {
			vm.ChangeSummary = v
		}
		if v, ok := row["created_by"].(string); ok {
			vm.CreatedBy = v
		}
		if v, ok := row["created_at"]; ok {
			switch val := v.(type) {
			case string:
				vm.CreatedAt = val
			case []byte:
				vm.CreatedAt = string(val)
			}
		}
		versions = append(versions, vm)
	}

	return versions, nil
}

// GetVersion returns the full YAML for a specific version.
func (r *VersionRepository) GetVersion(teamID int64, pageID string, version int) (string, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT config_yaml FROM dashboard_config_versions
		WHERE team_id = ? AND page_id = ? AND version = ?
	`, teamID, pageID, version)
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
