package dashboardconfig

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// ShareRecord holds a dashboard share link record.
type ShareRecord struct {
	ShareID    string  `json:"shareId"`
	TeamID     int64   `json:"teamId"`
	PageID     string  `json:"pageId"`
	ParamsJSON string  `json:"paramsJson"`
	CreatedBy  string  `json:"createdBy"`
	CreatedAt  string  `json:"createdAt"`
	ExpiresAt  *string `json:"expiresAt,omitempty"`
}

// ShareRepository handles dashboard share link persistence.
type ShareRepository struct {
	db dbutil.Querier
}

// NewShareRepository creates a new ShareRepository.
func NewShareRepository(db *sql.DB) *ShareRepository {
	return &ShareRepository{db: dbutil.NewMySQLWrapper(db)}
}

// EnsureTable creates the dashboard_shares table if it does not exist.
func (r *ShareRepository) EnsureTable() error {
	_, err := r.db.Exec(`
		CREATE TABLE IF NOT EXISTS dashboard_shares (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			share_id VARCHAR(12) NOT NULL UNIQUE,
			team_id BIGINT NOT NULL,
			page_id VARCHAR(100) NOT NULL,
			params_json TEXT NOT NULL,
			created_by VARCHAR(255) DEFAULT '',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			expires_at TIMESTAMP NULL,
			INDEX idx_share_id (share_id)
		)
	`)
	return err
}

// GenerateShareID generates a random 12-character hex share ID.
func GenerateShareID() string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		// Fallback to timestamp-based ID
		return hex.EncodeToString([]byte(time.Now().Format("060102150405")))[:12]
	}
	return hex.EncodeToString(b)
}

// CreateShare inserts a new share link record.
func (r *ShareRepository) CreateShare(teamID int64, pageID, shareID, paramsJSON, createdBy string, expiresAt *time.Time) error {
	if expiresAt != nil {
		_, err := r.db.Exec(`
			INSERT INTO dashboard_shares (share_id, team_id, page_id, params_json, created_by, expires_at)
			VALUES (?, ?, ?, ?, ?, ?)
		`, shareID, teamID, pageID, paramsJSON, createdBy, *expiresAt)
		return err
	}

	_, err := r.db.Exec(`
		INSERT INTO dashboard_shares (share_id, team_id, page_id, params_json, created_by)
		VALUES (?, ?, ?, ?, ?)
	`, shareID, teamID, pageID, paramsJSON, createdBy)
	return err
}

// GetShare retrieves a share record by its share ID.
// Returns nil if not found or expired.
func (r *ShareRepository) GetShare(shareID string) (*ShareRecord, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT share_id, team_id, page_id, params_json, created_by, created_at, expires_at
		FROM dashboard_shares
		WHERE share_id = ? AND (expires_at IS NULL OR expires_at > NOW())
	`, shareID)
	if err != nil {
		return nil, err
	}

	if len(row) == 0 {
		return nil, nil
	}

	record := &ShareRecord{}
	if v, ok := row["share_id"].(string); ok {
		record.ShareID = v
	}
	if v, ok := row["team_id"].(int64); ok {
		record.TeamID = v
	}
	if v, ok := row["page_id"].(string); ok {
		record.PageID = v
	}
	if v, ok := row["params_json"].(string); ok {
		record.ParamsJSON = v
	}
	if v, ok := row["created_by"].(string); ok {
		record.CreatedBy = v
	}
	if v, ok := row["created_at"]; ok {
		switch val := v.(type) {
		case string:
			record.CreatedAt = val
		case []byte:
			record.CreatedAt = string(val)
		}
	}
	if v, ok := row["expires_at"]; ok && v != nil {
		switch val := v.(type) {
		case string:
			record.ExpiresAt = &val
		case []byte:
			s := string(val)
			record.ExpiresAt = &s
		}
	}

	return record, nil
}
