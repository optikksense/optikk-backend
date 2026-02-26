package impl

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/observability/observability-backend-go/internal/contracts"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/alerts/model"
)

// ClickHouseRepository encapsulates data access logic for alerts and incidents.
type ClickHouseRepository struct {
	db           dbutil.Querier
	alertCondCol string
}

// NewRepository creates a new Alerts Repository.
func NewRepository(db *sql.DB, alertCondCol string) *ClickHouseRepository {
	return &ClickHouseRepository{
		db:           dbutil.NewMySQLWrapper(db),
		alertCondCol: alertCondCol,
	}
}

func (r *ClickHouseRepository) selectSQL() string {
	return fmt.Sprintf(`
		SELECT id, organization_id, team_id, name, description, type, severity, status,
		       service_name, %s as condition_expr, metric, operator, threshold, duration_minutes,
		       current_value, triggered_by, triggered_at, acknowledged_at, acknowledged_by,
		       resolved_at, resolved_by, muted_until, mute_reason, runbook_url, created_at, updated_at
		FROM alerts`, r.alertCondCol)
}

func mapToAlert(row map[string]any) model.Alert {
	trigger := dbutil.NullableTimeFromAny(row["triggered_at"])
	if trigger == nil {
		trigger = dbutil.NullableTimeFromAny(row["created_at"])
	}
	return model.Alert{
		ID:              dbutil.Int64FromAny(row["id"]),
		OrganizationID:  dbutil.Int64FromAny(row["organization_id"]),
		TeamID:          dbutil.Int64FromAny(row["team_id"]),
		Name:            dbutil.StringFromAny(row["name"]),
		Description:     dbutil.NullableStringFromAny(row["description"]),
		Type:            dbutil.StringFromAny(row["type"]),
		Severity:        dbutil.StringFromAny(row["severity"]),
		Status:          dbutil.StringFromAny(row["status"]),
		ServiceName:     dbutil.NullableStringFromAny(row["service_name"]),
		Condition:       dbutil.NullableStringFromAny(row["condition_expr"]),
		Metric:          dbutil.NullableStringFromAny(row["metric"]),
		Operator:        dbutil.NullableStringFromAny(row["operator"]),
		Threshold:       dbutil.Float64FromAny(row["threshold"]),
		DurationMinutes: int(dbutil.Int64FromAny(row["duration_minutes"])),
		CurrentValue:    dbutil.NullableFloat64FromAny(row["current_value"]),
		TriggeredBy:     dbutil.NullableStringFromAny(row["triggered_by"]),
		TriggeredAt:     trigger,
		AcknowledgedAt:  dbutil.NullableTimeFromAny(row["acknowledged_at"]),
		AcknowledgedBy:  dbutil.NullableStringFromAny(row["acknowledged_by"]),
		ResolvedAt:      dbutil.NullableTimeFromAny(row["resolved_at"]),
		ResolvedBy:      dbutil.NullableStringFromAny(row["resolved_by"]),
		MutedUntil:      dbutil.NullableTimeFromAny(row["muted_until"]),
		MuteReason:      dbutil.NullableStringFromAny(row["mute_reason"]),
		RunbookURL:      dbutil.NullableStringFromAny(row["runbook_url"]),
		CreatedAt:       dbutil.TimeFromAny(row["created_at"]),
		UpdatedAt:       dbutil.NullableTimeFromAny(row["updated_at"]),
	}
}

func mapRowsToAlerts(rows []map[string]any) []model.Alert {
	alerts := make([]model.Alert, len(rows))
	for i, row := range rows {
		alerts[i] = mapToAlert(row)
	}
	return alerts
}

func (r *ClickHouseRepository) GetAlerts(teamID int64, status string) ([]model.Alert, error) {
	query := r.selectSQL() + ` WHERE team_id = ?`
	args := []any{teamID}
	if status != "" {
		query += ` AND LOWER(status) = LOWER(?)`
		args = append(args, status)
	}
	query += ` ORDER BY created_at DESC`
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	return mapRowsToAlerts(rows), nil
}

func (r *ClickHouseRepository) GetAlertsPaged(teamID int64, limit, offset int) ([]model.Alert, int64, error) {
	query := r.selectSQL() + `
		WHERE team_id = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?`
	rows, err := dbutil.QueryMaps(r.db, query, teamID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM alerts WHERE team_id = ?`, teamID)
	return mapRowsToAlerts(rows), total, nil
}

func (r *ClickHouseRepository) GetAlertByID(id int64) (*model.Alert, error) {
	row, err := dbutil.QueryMap(r.db, r.selectSQL()+` WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	alert := mapToAlert(row)
	return &alert, nil
}

func (r *ClickHouseRepository) CreateAlert(orgID, teamID int64, req contracts.AlertRequest) (int64, error) {
	insertSQL := fmt.Sprintf(`
		INSERT INTO alerts (
			organization_id, team_id, name, description, type, severity, status,
			service_name, %s, metric, operator, threshold, duration_minutes, runbook_url, created_at
		) VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?)
	`, r.alertCondCol)

	res, err := r.db.Exec(insertSQL,
		orgID, teamID, req.Name, dbutil.NullableString(req.Description),
		req.Type, req.Severity, dbutil.NullableString(req.ServiceName), dbutil.NullableString(req.Condition),
		dbutil.NullableString(req.Metric), dbutil.NullableString(req.Operator),
		req.Threshold, req.DurationMinutes, dbutil.NullableString(req.RunbookURL), time.Now().UTC())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (r *ClickHouseRepository) MuteAlertWithReason(id int64, minutes int, reason string) error {
	_, err := r.db.Exec(`
		UPDATE alerts
		SET status = 'muted', muted_until = ?, mute_reason = ?, updated_at = ?
		WHERE id = ?
	`, time.Now().UTC().Add(time.Duration(minutes)*time.Minute), reason, time.Now().UTC(), id)
	return err
}

func (r *ClickHouseRepository) BulkMuteAlerts(ids []int64, minutes int, reason string) ([]model.Alert, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	now := time.Now().UTC()
	mutedUntil := now.Add(time.Duration(minutes) * time.Minute)

	inClause, inArgs := dbutil.InClauseInt64(ids)
	updateArgs := append([]any{mutedUntil, reason, now}, inArgs...)
	if _, err := r.db.Exec(`UPDATE alerts SET status = 'muted', muted_until = ?, mute_reason = ?, updated_at = ? WHERE id IN `+inClause, updateArgs...); err != nil {
		return nil, err
	}

	rows, err := dbutil.QueryMaps(r.db, r.selectSQL()+` WHERE id IN `+inClause, inArgs...)
	if err != nil {
		return nil, err
	}
	return mapRowsToAlerts(rows), nil
}

func (r *ClickHouseRepository) BulkResolveAlerts(ids []int64, resolvedBy string) ([]model.Alert, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	now := time.Now().UTC()

	inClause, inArgs := dbutil.InClauseInt64(ids)
	updateArgs := append([]any{now, resolvedBy, now}, inArgs...)
	if _, err := r.db.Exec(`UPDATE alerts SET status = 'resolved', resolved_at = ?, resolved_by = ?, updated_at = ? WHERE id IN `+inClause, updateArgs...); err != nil {
		return nil, err
	}

	rows, err := dbutil.QueryMaps(r.db, r.selectSQL()+` WHERE id IN `+inClause, inArgs...)
	if err != nil {
		return nil, err
	}
	return mapRowsToAlerts(rows), nil
}

func (r *ClickHouseRepository) GetAlertsForIncident(teamID int64, policyID string) ([]model.Alert, error) {
	rows, err := dbutil.QueryMaps(r.db, r.selectSQL()+`
		WHERE team_id = ? AND triggered_by LIKE ?
		ORDER BY created_at DESC
	`, teamID, "%"+policyID+"%")
	if err != nil {
		return nil, err
	}
	return mapRowsToAlerts(rows), nil
}

func (r *ClickHouseRepository) CountActiveAlerts(teamID int64) int64 {
	return dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM alerts WHERE team_id = ? AND status = 'active'`, teamID)
}

func incidentStatusFromAlertStatus(status string) string {
	switch status {
	case "active":
		return "open"
	case "acknowledged":
		return "investigating"
	case "muted":
		return "monitoring"
	case "resolved":
		return "resolved"
	default:
		return status
	}
}

func (r *ClickHouseRepository) GetIncidents(teamID int64, startMs, endMs int64, statuses, severities, services []string, limit, offset int) ([]model.Incident, int64, map[string]int64, map[string]int64, error) {
	queryBase := `
		FROM alerts
		WHERE team_id = ?
		  AND created_at BETWEEN ? AND ?`
	baseArgs := []any{teamID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}

	if len(statuses) > 0 {
		in, vals := dbutil.InClauseFromStrings(statuses)
		queryBase += ` AND status IN ` + in
		baseArgs = append(baseArgs, vals...)
	}
	if len(severities) > 0 {
		in, vals := dbutil.InClauseFromStrings(severities)
		queryBase += ` AND severity IN ` + in
		baseArgs = append(baseArgs, vals...)
	}
	if len(services) > 0 {
		in, vals := dbutil.InClauseFromStrings(services)
		queryBase += ` AND service_name IN ` + in
		baseArgs = append(baseArgs, vals...)
	}

	query := `
		SELECT id, triggered_by, name, description, severity, status, type, service_name,
		       created_at, updated_at, resolved_at, acknowledged_at, acknowledged_by, triggered_at
	` + queryBase + `
		ORDER BY created_at DESC LIMIT ? OFFSET ?`
	args := append(append([]any{}, baseArgs...), limit, offset)

	items, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, 0, nil, nil, err
	}

	incidents := make([]model.Incident, len(items))
	for i, alert := range items {
		createdAt := dbutil.TimeFromAny(alert["created_at"])
		if alert["triggered_at"] != nil {
			createdAt = dbutil.TimeFromAny(alert["triggered_at"])
		}
		incidents[i] = model.Incident{
			IncidentID:     dbutil.Int64FromAny(alert["id"]),
			AlertPolicyID:  dbutil.NullableStringFromAny(alert["triggered_by"]),
			TriggeredBy:    dbutil.NullableStringFromAny(alert["triggered_by"]),
			Title:          dbutil.StringFromAny(alert["name"]),
			Description:    dbutil.NullableStringFromAny(alert["description"]),
			Severity:       dbutil.StringFromAny(alert["severity"]),
			Status:         incidentStatusFromAlertStatus(dbutil.StringFromAny(alert["status"])),
			Source:         dbutil.StringFromAny(alert["type"]),
			ServiceName:    dbutil.NullableStringFromAny(alert["service_name"]),
			CreatedAt:      createdAt,
			UpdatedAt:      dbutil.NullableTimeFromAny(alert["updated_at"]),
			ResolvedAt:     dbutil.NullableTimeFromAny(alert["resolved_at"]),
			AcknowledgedAt: dbutil.NullableTimeFromAny(alert["acknowledged_at"]),
			AcknowledgedBy: dbutil.NullableStringFromAny(alert["acknowledged_by"]),
		}
	}

	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) `+queryBase, baseArgs...)

	allRows, _ := dbutil.QueryMaps(r.db, `SELECT status, severity `+queryBase, baseArgs...)
	statusCounts := map[string]int64{}
	severityCounts := map[string]int64{}
	for _, row := range allRows {
		status := incidentStatusFromAlertStatus(dbutil.StringFromAny(row["status"]))
		statusCounts[status]++
		severityCounts[dbutil.StringFromAny(row["severity"])]++
	}

	return incidents, total, statusCounts, severityCounts, nil
}

func (r *ClickHouseRepository) UpdateAlertState(id int64, status string, acknowledge, resolve, mute bool, user string, muteMinutes int) error {
	now := time.Now().UTC()
	sqlQ := `UPDATE alerts SET status = ?, updated_at = ?`
	args := []any{status, now}
	if acknowledge {
		sqlQ += `, acknowledged_at = ?, acknowledged_by = ?`
		args = append(args, now, user)
	}
	if resolve {
		sqlQ += `, resolved_at = ?, resolved_by = ?`
		args = append(args, now, user)
	}
	if mute {
		sqlQ += `, muted_until = ?`
		args = append(args, now.Add(time.Duration(muteMinutes)*time.Minute))
	}
	sqlQ += ` WHERE id = ?`
	args = append(args, id)

	_, err := r.db.Exec(sqlQ, args...)
	return err
}
