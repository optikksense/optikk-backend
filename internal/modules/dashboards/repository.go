package dashboards

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

func (r *Repository) Create(teamID, userID int64, req CreateDashboardRequest) (*Dashboard, error) {
	result, err := r.db.Exec(`
		INSERT INTO custom_dashboards (team_id, created_by, name, description, is_shared, layout_json)
		VALUES (?, ?, ?, ?, ?, '{}')
	`, teamID, userID, req.Name, req.Description, req.IsShared)
	if err != nil {
		return nil, err
	}
	id, _ := result.LastInsertId()
	return r.GetByID(teamID, id)
}

func (r *Repository) GetByID(teamID, id int64) (*Dashboard, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, team_id, created_by, name, description, is_shared, layout_json, created_at, updated_at
		FROM custom_dashboards WHERE id = ? AND team_id = ?
	`, id, teamID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return dashboardFromRow(row), nil
}

func (r *Repository) List(teamID, userID int64) ([]Dashboard, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT id, team_id, created_by, name, description, is_shared, layout_json, created_at, updated_at
		FROM custom_dashboards
		WHERE team_id = ? AND (created_by = ? OR is_shared = true)
		ORDER BY updated_at DESC
	`, teamID, userID)
	if err != nil {
		return nil, err
	}
	dashboards := make([]Dashboard, 0, len(rows))
	for _, row := range rows {
		dashboards = append(dashboards, *dashboardFromRow(row))
	}
	return dashboards, nil
}

func (r *Repository) Update(teamID, id int64, req UpdateDashboardRequest) (*Dashboard, error) {
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
	if req.IsShared != nil {
		setParts = append(setParts, "is_shared = ?")
		args = append(args, *req.IsShared)
	}
	if req.LayoutJSON != nil {
		setParts = append(setParts, "layout_json = ?")
		args = append(args, *req.LayoutJSON)
	}
	if len(setParts) == 0 {
		return r.GetByID(teamID, id)
	}

	query := "UPDATE custom_dashboards SET "
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
	result, err := r.db.Exec(`DELETE FROM custom_dashboards WHERE id = ? AND team_id = ?`, id, teamID)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("dashboard not found")
	}
	return nil
}

func dashboardFromRow(row map[string]any) *Dashboard {
	return &Dashboard{
		ID:          dbutil.Int64FromAny(row["id"]),
		TeamID:      dbutil.Int64FromAny(row["team_id"]),
		CreatedBy:   dbutil.Int64FromAny(row["created_by"]),
		Name:        dbutil.StringFromAny(row["name"]),
		Description: dbutil.StringFromAny(row["description"]),
		IsShared:    dbutil.BoolFromAny(row["is_shared"]),
		LayoutJSON:  dbutil.StringFromAny(row["layout_json"]),
		CreatedAt:   dbutil.TimeFromAny(row["created_at"]),
		UpdatedAt:   dbutil.TimeFromAny(row["updated_at"]),
	}
}
