package alerting

import (
	"database/sql"
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// Repository handles alert rules and notification channels persistence.
type Repository struct {
	db dbutil.Querier
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: dbutil.NewMySQLWrapper(db)}
}

// --- Alert Rules ---

func (r *Repository) CreateRule(teamID, userID int64, req CreateRuleRequest) (*AlertRule, error) {
	result, err := r.db.Exec(`
		INSERT INTO alert_rules (team_id, created_by, name, description, enabled, severity, condition_type, signal_type, query, operator, threshold, duration_minutes, service_name)
		VALUES (?, ?, ?, ?, true, ?, ?, ?, ?, ?, ?, ?, ?)
	`, teamID, userID, req.Name, req.Description, req.Severity, req.ConditionType, req.SignalType, req.Query, req.Operator, req.Threshold, req.DurationMinutes, req.ServiceName)
	if err != nil {
		return nil, err
	}
	id, _ := result.LastInsertId()
	return r.GetRuleByID(teamID, id)
}

func (r *Repository) GetRuleByID(teamID, id int64) (*AlertRule, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, team_id, created_by, name, description, enabled, severity, condition_type, signal_type, query, operator, threshold, duration_minutes, service_name, created_at, updated_at
		FROM alert_rules WHERE id = ? AND team_id = ?
	`, id, teamID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return ruleFromRow(row), nil
}

func (r *Repository) ListRules(teamID int64) ([]AlertRule, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT id, team_id, created_by, name, description, enabled, severity, condition_type, signal_type, query, operator, threshold, duration_minutes, service_name, created_at, updated_at
		FROM alert_rules WHERE team_id = ? ORDER BY updated_at DESC
	`, teamID)
	if err != nil {
		return nil, err
	}
	rules := make([]AlertRule, 0, len(rows))
	for _, row := range rows {
		rules = append(rules, *ruleFromRow(row))
	}
	return rules, nil
}

func (r *Repository) ListEnabledRules(teamID int64) ([]AlertRule, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT id, team_id, created_by, name, description, enabled, severity, condition_type, signal_type, query, operator, threshold, duration_minutes, service_name, created_at, updated_at
		FROM alert_rules WHERE team_id = ? AND enabled = true ORDER BY updated_at DESC
	`, teamID)
	if err != nil {
		return nil, err
	}
	rules := make([]AlertRule, 0, len(rows))
	for _, row := range rows {
		rules = append(rules, *ruleFromRow(row))
	}
	return rules, nil
}

func (r *Repository) UpdateRule(teamID, id int64, req UpdateRuleRequest) (*AlertRule, error) {
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
	if req.Enabled != nil {
		setParts = append(setParts, "enabled = ?")
		args = append(args, *req.Enabled)
	}
	if req.Severity != nil {
		setParts = append(setParts, "severity = ?")
		args = append(args, *req.Severity)
	}
	if req.Query != nil {
		setParts = append(setParts, "query = ?")
		args = append(args, *req.Query)
	}
	if req.Operator != nil {
		setParts = append(setParts, "operator = ?")
		args = append(args, *req.Operator)
	}
	if req.Threshold != nil {
		setParts = append(setParts, "threshold = ?")
		args = append(args, *req.Threshold)
	}
	if req.DurationMinutes != nil {
		setParts = append(setParts, "duration_minutes = ?")
		args = append(args, *req.DurationMinutes)
	}
	if req.ServiceName != nil {
		setParts = append(setParts, "service_name = ?")
		args = append(args, *req.ServiceName)
	}
	if len(setParts) == 0 {
		return r.GetRuleByID(teamID, id)
	}

	query := "UPDATE alert_rules SET "
	for i, p := range setParts {
		if i > 0 {
			query += ", "
		}
		query += p
	}
	query += " WHERE id = ? AND team_id = ?"
	args = append(args, id, teamID)

	if _, err := r.db.Exec(query, args...); err != nil {
		return nil, err
	}
	return r.GetRuleByID(teamID, id)
}

func (r *Repository) DeleteRule(teamID, id int64) error {
	result, err := r.db.Exec(`DELETE FROM alert_rules WHERE id = ? AND team_id = ?`, id, teamID)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("alert rule not found")
	}
	return nil
}

// --- Row mapper ---

func ruleFromRow(row map[string]any) *AlertRule {
	return &AlertRule{
		ID:              dbutil.Int64FromAny(row["id"]),
		TeamID:          dbutil.Int64FromAny(row["team_id"]),
		CreatedBy:       dbutil.Int64FromAny(row["created_by"]),
		Name:            dbutil.StringFromAny(row["name"]),
		Description:     dbutil.StringFromAny(row["description"]),
		Enabled:         dbutil.BoolFromAny(row["enabled"]),
		Severity:        dbutil.StringFromAny(row["severity"]),
		ConditionType:   dbutil.StringFromAny(row["condition_type"]),
		SignalType:      dbutil.StringFromAny(row["signal_type"]),
		Query:           dbutil.StringFromAny(row["query"]),
		Operator:        dbutil.StringFromAny(row["operator"]),
		Threshold:       dbutil.Float64FromAny(row["threshold"]),
		DurationMinutes: int(dbutil.Int64FromAny(row["duration_minutes"])),
		ServiceName:     dbutil.StringFromAny(row["service_name"]),
		CreatedAt:       dbutil.TimeFromAny(row["created_at"]),
		UpdatedAt:       dbutil.TimeFromAny(row["updated_at"]),
	}
}
