package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/explore/model"
)

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db *sql.DB) *ClickHouseRepository {
	return &ClickHouseRepository{db: dbutil.NewMySQLWrapper(db)}
}

func (r *ClickHouseRepository) EnsureTable() error {
	_, err := r.db.Exec(`
		CREATE TABLE IF NOT EXISTS explore_saved_queries (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			organization_id BIGINT NOT NULL,
			team_id BIGINT NOT NULL,
			query_type VARCHAR(20) NOT NULL,
			name VARCHAR(200) NOT NULL,
			description VARCHAR(500),
			query_payload TEXT NOT NULL,
			created_by_user_id BIGINT NOT NULL DEFAULT 0,
			created_by_email VARCHAR(255),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			INDEX idx_explore_saved_queries_team (team_id, query_type, updated_at),
			INDEX idx_explore_saved_queries_org (organization_id)
		)
	`)
	return err
}

func (r *ClickHouseRepository) ListSavedQueries(teamID int64, queryType string) ([]model.SavedQuery, error) {
	query := `
		SELECT id, organization_id, team_id, query_type, name, description, query_payload,
		       created_by_user_id, created_by_email, created_at, updated_at
		FROM explore_saved_queries
		WHERE team_id = ?`
	args := []any{teamID}

	if queryType != "" {
		query += ` AND query_type = ?`
		args = append(args, queryType)
	}

	query += ` ORDER BY updated_at DESC, id DESC LIMIT 500`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	items := make([]model.SavedQuery, 0, len(rows))
	for _, row := range rows {
		items = append(items, rowToSavedQuery(row))
	}
	return items, nil
}

func (r *ClickHouseRepository) CreateSavedQuery(in model.SavedQueryInput) (model.SavedQuery, error) {
	payload, err := marshalPayload(in.Query)
	if err != nil {
		return model.SavedQuery{}, err
	}

	res, err := r.db.Exec(`
		INSERT INTO explore_saved_queries (
			organization_id, team_id, query_type, name, description, query_payload,
			created_by_user_id, created_by_email
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, in.OrganizationID, in.TeamID, in.QueryType, in.Name, dbutil.NullableString(in.Description), payload,
		in.CreatedByUserID, dbutil.NullableString(in.CreatedByEmail))
	if err != nil {
		return model.SavedQuery{}, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return model.SavedQuery{}, err
	}
	return r.GetSavedQuery(in.TeamID, id)
}

func (r *ClickHouseRepository) UpdateSavedQuery(teamID, id int64, in model.SavedQueryInput) (model.SavedQuery, error) {
	payload, err := marshalPayload(in.Query)
	if err != nil {
		return model.SavedQuery{}, err
	}

	res, err := r.db.Exec(`
		UPDATE explore_saved_queries
		SET query_type = ?, name = ?, description = ?, query_payload = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ? AND team_id = ?
	`, in.QueryType, in.Name, dbutil.NullableString(in.Description), payload, id, teamID)
	if err != nil {
		return model.SavedQuery{}, err
	}

	if dbutil.RowsAffected(res) == 0 {
		return model.SavedQuery{}, sql.ErrNoRows
	}

	return r.GetSavedQuery(teamID, id)
}

func (r *ClickHouseRepository) DeleteSavedQuery(teamID, id int64) (bool, error) {
	res, err := r.db.Exec(`DELETE FROM explore_saved_queries WHERE id = ? AND team_id = ?`, id, teamID)
	if err != nil {
		return false, err
	}
	return dbutil.RowsAffected(res) > 0, nil
}

func (r *ClickHouseRepository) GetSavedQuery(teamID, id int64) (model.SavedQuery, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, organization_id, team_id, query_type, name, description, query_payload,
		       created_by_user_id, created_by_email, created_at, updated_at
		FROM explore_saved_queries
		WHERE id = ? AND team_id = ?
		LIMIT 1
	`, id, teamID)
	if err != nil {
		return model.SavedQuery{}, err
	}
	if len(row) == 0 {
		return model.SavedQuery{}, sql.ErrNoRows
	}
	return rowToSavedQuery(row), nil
}

func rowToSavedQuery(row map[string]any) model.SavedQuery {
	payload := parsePayload(dbutil.StringFromAny(row["query_payload"]))
	return model.SavedQuery{
		ID:              dbutil.Int64FromAny(row["id"]),
		OrganizationID:  dbutil.Int64FromAny(row["organization_id"]),
		TeamID:          dbutil.Int64FromAny(row["team_id"]),
		QueryType:       dbutil.StringFromAny(row["query_type"]),
		Name:            dbutil.StringFromAny(row["name"]),
		Description:     dbutil.StringFromAny(row["description"]),
		Query:           payload,
		CreatedByUserID: dbutil.Int64FromAny(row["created_by_user_id"]),
		CreatedByEmail:  dbutil.StringFromAny(row["created_by_email"]),
		CreatedAt:       dbutil.StringFromAny(row["created_at"]),
		UpdatedAt:       dbutil.StringFromAny(row["updated_at"]),
	}
}

func parsePayload(raw string) map[string]any {
	out := map[string]any{}
	if strings.TrimSpace(raw) == "" {
		return out
	}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return map[string]any{"raw": raw}
	}
	return out
}

func marshalPayload(v any) (string, error) {
	if v == nil {
		return "", fmt.Errorf("query payload is required")
	}
	blob, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	if len(blob) == 0 || string(blob) == "null" {
		return "", fmt.Errorf("query payload is required")
	}
	return string(blob), nil
}
