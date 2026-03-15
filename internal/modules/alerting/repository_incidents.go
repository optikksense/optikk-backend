package alerting

import (
	"fmt"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

func (r *Repository) CreateIncident(teamID int64, inc Incident) (*Incident, error) {
	result, err := r.db.Exec(`
		INSERT INTO alert_incidents (team_id, rule_id, rule_name, severity, status, trigger_value, threshold, message, triggered_at)
		VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?)
	`, teamID, inc.RuleID, inc.RuleName, inc.Severity, inc.TriggerValue, inc.Threshold, inc.Message, inc.TriggeredAt)
	if err != nil {
		return nil, err
	}
	id, _ := result.LastInsertId()
	return r.GetIncidentByID(teamID, id)
}

func (r *Repository) GetIncidentByID(teamID, id int64) (*Incident, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, team_id, rule_id, rule_name, severity, status, trigger_value, threshold, message,
		       acknowledged_by, resolved_by, triggered_at, acknowledged_at, resolved_at, created_at
		FROM alert_incidents WHERE id = ? AND team_id = ?
	`, id, teamID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return incidentFromRow(row), nil
}

func (r *Repository) ListIncidents(teamID int64, status string, limit int) ([]Incident, error) {
	query := `
		SELECT id, team_id, rule_id, rule_name, severity, status, trigger_value, threshold, message,
		       acknowledged_by, resolved_by, triggered_at, acknowledged_at, resolved_at, created_at
		FROM alert_incidents WHERE team_id = ?`
	args := []any{teamID}

	if status != "" {
		query += " AND status = ?"
		args = append(args, status)
	}
	query += " ORDER BY created_at DESC"
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	incidents := make([]Incident, 0, len(rows))
	for _, row := range rows {
		incidents = append(incidents, *incidentFromRow(row))
	}
	return incidents, nil
}

func (r *Repository) UpdateIncidentStatus(teamID, id int64, status string, userID int64) (*Incident, error) {
	now := time.Now()
	var query string
	var args []any

	switch status {
	case "acknowledged":
		query = `UPDATE alert_incidents SET status = 'acknowledged', acknowledged_by = ?, acknowledged_at = ? WHERE id = ? AND team_id = ?`
		args = []any{userID, now, id, teamID}
	case "resolved":
		query = `UPDATE alert_incidents SET status = 'resolved', resolved_by = ?, resolved_at = ? WHERE id = ? AND team_id = ?`
		args = []any{userID, now, id, teamID}
	default:
		return nil, fmt.Errorf("invalid incident status: %s", status)
	}

	result, err := r.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return nil, fmt.Errorf("incident not found")
	}
	return r.GetIncidentByID(teamID, id)
}

func (r *Repository) CountOpenIncidents(teamID int64) (int64, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT COUNT(*) as cnt FROM alert_incidents WHERE team_id = ? AND status = 'open'
	`, teamID)
	if err != nil {
		return 0, err
	}
	if row == nil {
		return 0, nil
	}
	return dbutil.Int64FromAny(row["cnt"]), nil
}

func nullableInt64FromAny(v any) *int64 {
	if v == nil {
		return nil
	}
	val := dbutil.Int64FromAny(v)
	if val == 0 {
		return nil
	}
	return &val
}

func incidentFromRow(row map[string]any) *Incident {
	return &Incident{
		ID:             dbutil.Int64FromAny(row["id"]),
		TeamID:         dbutil.Int64FromAny(row["team_id"]),
		RuleID:         dbutil.Int64FromAny(row["rule_id"]),
		RuleName:       dbutil.StringFromAny(row["rule_name"]),
		Severity:       dbutil.StringFromAny(row["severity"]),
		Status:         dbutil.StringFromAny(row["status"]),
		TriggerValue:   dbutil.Float64FromAny(row["trigger_value"]),
		Threshold:      dbutil.Float64FromAny(row["threshold"]),
		Message:        dbutil.StringFromAny(row["message"]),
		AcknowledgedBy: nullableInt64FromAny(row["acknowledged_by"]),
		ResolvedBy:     nullableInt64FromAny(row["resolved_by"]),
		TriggeredAt:    dbutil.TimeFromAny(row["triggered_at"]),
		AcknowledgedAt: dbutil.NullableTimeFromAny(row["acknowledged_at"]),
		ResolvedAt:     dbutil.NullableTimeFromAny(row["resolved_at"]),
		CreatedAt:      dbutil.TimeFromAny(row["created_at"]),
	}
}
