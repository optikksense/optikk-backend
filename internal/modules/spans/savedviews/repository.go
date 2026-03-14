package savedviews

import (
	"database/sql"
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type Repository struct {
	db dbutil.Querier
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: dbutil.NewMySQLWrapper(db)}
}

func (r *Repository) Create(teamID, userID int64, req CreateViewRequest) (*SavedView, error) {
	result, err := r.db.Exec(`
		INSERT INTO saved_views (team_id, created_by, name, description, view_type, query_json, is_shared)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, teamID, userID, req.Name, req.Description, req.ViewType, req.QueryJSON, req.IsShared)
	if err != nil {
		return nil, err
	}
	id, _ := result.LastInsertId()
	return r.GetByID(teamID, id)
}

func (r *Repository) GetByID(teamID, id int64) (*SavedView, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, team_id, created_by, name, description, view_type, query_json, is_shared, created_at, updated_at
		FROM saved_views WHERE id = ? AND team_id = ?
	`, id, teamID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return viewFromRow(row), nil
}

func (r *Repository) List(teamID, userID int64) ([]SavedView, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT id, team_id, created_by, name, description, view_type, query_json, is_shared, created_at, updated_at
		FROM saved_views
		WHERE team_id = ? AND (created_by = ? OR is_shared = true)
		ORDER BY updated_at DESC
	`, teamID, userID)
	if err != nil {
		return nil, err
	}
	views := make([]SavedView, 0, len(rows))
	for _, row := range rows {
		views = append(views, *viewFromRow(row))
	}
	return views, nil
}

func (r *Repository) Update(teamID, id int64, req UpdateViewRequest) (*SavedView, error) {
	setParts := []string{}
	args := []any{}
	if req.Name != nil {
		setParts = append(setParts, "name = ?")
		args = append(args, *req.Name)
	}
	if req.Description != nil {
		setParts = append(setParts, "description = ?")
		args = append(args, *req.Description)
	}
	if req.QueryJSON != nil {
		setParts = append(setParts, "query_json = ?")
		args = append(args, *req.QueryJSON)
	}
	if req.IsShared != nil {
		setParts = append(setParts, "is_shared = ?")
		args = append(args, *req.IsShared)
	}
	if len(setParts) == 0 {
		return r.GetByID(teamID, id)
	}

	query := "UPDATE saved_views SET "
	for i, p := range setParts {
		if i > 0 {
			query += ", "
		}
		query += p
	}
	query += " WHERE id = ? AND team_id = ?"
	args = append(args, id, teamID)

	_, err := r.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return r.GetByID(teamID, id)
}

func (r *Repository) Delete(teamID, id int64) error {
	result, err := r.db.Exec(`DELETE FROM saved_views WHERE id = ? AND team_id = ?`, id, teamID)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("saved view not found")
	}
	return nil
}

func viewFromRow(row map[string]any) *SavedView {
	return &SavedView{
		ID:          dbutil.Int64FromAny(row["id"]),
		TeamID:      dbutil.Int64FromAny(row["team_id"]),
		CreatedBy:   dbutil.Int64FromAny(row["created_by"]),
		Name:        dbutil.StringFromAny(row["name"]),
		Description: dbutil.StringFromAny(row["description"]),
		ViewType:    dbutil.StringFromAny(row["view_type"]),
		QueryJSON:   dbutil.StringFromAny(row["query_json"]),
		IsShared:    dbutil.BoolFromAny(row["is_shared"]),
		CreatedAt:   dbutil.TimeFromAny(row["created_at"]),
		UpdatedAt:   dbutil.TimeFromAny(row["updated_at"]),
	}
}
