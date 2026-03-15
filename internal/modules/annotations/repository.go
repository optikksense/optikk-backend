package annotations

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

func (r *Repository) Create(teamID, userID int64, req CreateRequest) (*Annotation, error) {
	ts := dbutil.SqlTime(req.Timestamp)
	var endTime any
	if req.EndTime != nil {
		endTime = dbutil.SqlTime(*req.EndTime)
	}

	source := req.Source
	if source == "" {
		source = "manual"
	}

	result, err := r.db.Exec(`
		INSERT INTO annotations (team_id, created_by, title, description, timestamp, end_time, tags, source, service_name)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, teamID, userID, req.Title, req.Description, ts, endTime, req.Tags, source, req.ServiceName)
	if err != nil {
		return nil, err
	}
	id, _ := result.LastInsertId()
	return r.GetByID(teamID, id)
}

func (r *Repository) GetByID(teamID, id int64) (*Annotation, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, team_id, created_by, title, description, timestamp, end_time, tags, source, service_name, created_at, updated_at
		FROM annotations WHERE id = ? AND team_id = ?
	`, id, teamID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return annotationFromRow(row), nil
}

func (r *Repository) List(teamID int64, startMs, endMs int64, serviceName string) ([]Annotation, error) {
	query := `
		SELECT id, team_id, created_by, title, description, timestamp, end_time, tags, source, service_name, created_at, updated_at
		FROM annotations
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?`
	args := []any{teamID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}

	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` ORDER BY timestamp DESC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	out := make([]Annotation, 0, len(rows))
	for _, row := range rows {
		out = append(out, *annotationFromRow(row))
	}
	return out, nil
}

func (r *Repository) Update(teamID, id int64, req UpdateRequest) (*Annotation, error) {
	setParts := []string{}
	args := []any{}
	if req.Title != nil {
		setParts = append(setParts, "title = ?")
		args = append(args, *req.Title)
	}
	if req.Description != nil {
		setParts = append(setParts, "description = ?")
		args = append(args, *req.Description)
	}
	if req.Tags != nil {
		setParts = append(setParts, "tags = ?")
		args = append(args, *req.Tags)
	}
	if req.ServiceName != nil {
		setParts = append(setParts, "service_name = ?")
		args = append(args, *req.ServiceName)
	}
	if len(setParts) == 0 {
		return r.GetByID(teamID, id)
	}

	query := "UPDATE annotations SET "
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
	result, err := r.db.Exec(`DELETE FROM annotations WHERE id = ? AND team_id = ?`, id, teamID)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("annotation not found")
	}
	return nil
}

func annotationFromRow(row map[string]any) *Annotation {
	a := &Annotation{
		ID:          dbutil.Int64FromAny(row["id"]),
		TeamID:      dbutil.Int64FromAny(row["team_id"]),
		CreatedBy:   dbutil.Int64FromAny(row["created_by"]),
		Title:       dbutil.StringFromAny(row["title"]),
		Description: dbutil.StringFromAny(row["description"]),
		Timestamp:   dbutil.TimeFromAny(row["timestamp"]),
		Tags:        dbutil.StringFromAny(row["tags"]),
		Source:      dbutil.StringFromAny(row["source"]),
		ServiceName: dbutil.StringFromAny(row["service_name"]),
		CreatedAt:   dbutil.TimeFromAny(row["created_at"]),
		UpdatedAt:   dbutil.TimeFromAny(row["updated_at"]),
	}
	et := dbutil.NullableTimeFromAny(row["end_time"])
	if et != nil {
		a.EndTime = et
	}
	return a
}
