package sharing

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type Repository struct {
	db dbutil.Querier
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: dbutil.NewMySQLWrapper(db)}
}

func GenerateToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (r *Repository) Create(teamID, userID int64, token, resourceType, resourceID string, expiresAt any) (*SharedLink, error) {
	result, err := r.db.Exec(`
		INSERT INTO shared_links (team_id, created_by, token, resource_type, resource_id, expires_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, teamID, userID, token, resourceType, resourceID, expiresAt)
	if err != nil {
		return nil, err
	}
	id, _ := result.LastInsertId()
	return r.getByID(teamID, id)
}

func (r *Repository) GetByToken(token string) (*SharedLink, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, team_id, created_by, token, resource_type, resource_id, expires_at, created_at
		FROM shared_links WHERE token = ?
	`, token)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return linkFromRow(row), nil
}

func (r *Repository) ListByTeam(teamID int64) ([]SharedLink, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT id, team_id, created_by, token, resource_type, resource_id, expires_at, created_at
		FROM shared_links WHERE team_id = ?
		ORDER BY created_at DESC
	`, teamID)
	if err != nil {
		return nil, err
	}
	out := make([]SharedLink, 0, len(rows))
	for _, row := range rows {
		out = append(out, *linkFromRow(row))
	}
	return out, nil
}

func (r *Repository) Delete(teamID, id int64) error {
	result, err := r.db.Exec(`DELETE FROM shared_links WHERE id = ? AND team_id = ?`, id, teamID)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("shared link not found")
	}
	return nil
}

func (r *Repository) getByID(teamID, id int64) (*SharedLink, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, team_id, created_by, token, resource_type, resource_id, expires_at, created_at
		FROM shared_links WHERE id = ? AND team_id = ?
	`, id, teamID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return linkFromRow(row), nil
}

func linkFromRow(row map[string]any) *SharedLink {
	link := &SharedLink{
		ID:           dbutil.Int64FromAny(row["id"]),
		TeamID:       dbutil.Int64FromAny(row["team_id"]),
		CreatedBy:    dbutil.Int64FromAny(row["created_by"]),
		Token:        dbutil.StringFromAny(row["token"]),
		ResourceType: dbutil.StringFromAny(row["resource_type"]),
		ResourceID:   dbutil.StringFromAny(row["resource_id"]),
		CreatedAt:    dbutil.TimeFromAny(row["created_at"]),
	}
	if ea := dbutil.NullableTimeFromAny(row["expires_at"]); ea != nil {
		link.ExpiresAt = ea
	}
	return link
}
