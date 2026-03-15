package workloads

import (
	"database/sql"
	"encoding/json"
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type Repository struct {
	db dbutil.Querier
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: dbutil.NewMySQLWrapper(db)}
}

func (r *Repository) Create(teamID, userID int64, req CreateWorkloadRequest) (*Workload, error) {
	serviceNamesJSON, err := json.Marshal(req.ServiceNames)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal service names: %w", err)
	}

	result, err := r.db.Exec(`
		INSERT INTO workloads (team_id, created_by, name, description, service_names)
		VALUES (?, ?, ?, ?, ?)
	`, teamID, userID, req.Name, req.Description, string(serviceNamesJSON))
	if err != nil {
		return nil, err
	}

	id, _ := result.LastInsertId()
	return r.GetByID(teamID, id)
}

func (r *Repository) GetByID(teamID, id int64) (*Workload, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, team_id, created_by, name, description, service_names, created_at, updated_at
		FROM workloads WHERE id = ? AND team_id = ?
	`, id, teamID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return workloadFromRow(row), nil
}

func (r *Repository) List(teamID int64) ([]Workload, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT id, team_id, created_by, name, description, service_names, created_at, updated_at
		FROM workloads WHERE team_id = ?
		ORDER BY updated_at DESC
	`, teamID)
	if err != nil {
		return nil, err
	}

	workloads := make([]Workload, 0, len(rows))
	for _, row := range rows {
		workloads = append(workloads, *workloadFromRow(row))
	}
	return workloads, nil
}

func (r *Repository) Update(teamID, id int64, req UpdateWorkloadRequest) (*Workload, error) {
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
	if req.ServiceNames != nil {
		serviceNamesJSON, err := json.Marshal(*req.ServiceNames)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal service names: %w", err)
		}
		setParts = append(setParts, "service_names = ?")
		args = append(args, string(serviceNamesJSON))
	}

	if len(setParts) == 0 {
		return r.GetByID(teamID, id)
	}

	query := "UPDATE workloads SET "
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
	result, err := r.db.Exec(`DELETE FROM workloads WHERE id = ? AND team_id = ?`, id, teamID)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("workload not found")
	}
	return nil
}

func workloadFromRow(row map[string]any) *Workload {
	var serviceNames []string
	raw := dbutil.StringFromAny(row["service_names"])
	if raw != "" {
		_ = json.Unmarshal([]byte(raw), &serviceNames)
	}
	if serviceNames == nil {
		serviceNames = []string{}
	}

	return &Workload{
		ID:           dbutil.Int64FromAny(row["id"]),
		TeamID:       dbutil.Int64FromAny(row["team_id"]),
		CreatedBy:    dbutil.Int64FromAny(row["created_by"]),
		Name:         dbutil.StringFromAny(row["name"]),
		Description:  dbutil.StringFromAny(row["description"]),
		ServiceNames: serviceNames,
		CreatedAt:    dbutil.TimeFromAny(row["created_at"]),
		UpdatedAt:    dbutil.TimeFromAny(row["updated_at"]),
	}
}
