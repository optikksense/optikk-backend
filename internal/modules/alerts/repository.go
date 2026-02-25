package alerts

import (
	"database/sql"
	"fmt"
	"time"

	types "github.com/observability/observability-backend-go/internal/contracts"
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// Repository encapsulates data access logic for alerts and incidents.
type Repository struct {
	db           dbutil.Querier
	alertCondCol string
}

// NewRepository creates a new Alerts Repository.
func NewRepository(db *sql.DB, alertCondCol string) *Repository {
	return &Repository{
		db:           dbutil.NewMySQLWrapper(db),
		alertCondCol: alertCondCol,
	}
}

func (r *Repository) selectSQL() string {
	return fmt.Sprintf(`
		SELECT id, organization_id, team_id, name, description, type, severity, status,
		       service_name, %s as condition_expr, metric, operator, threshold, duration_minutes,
		       current_value, triggered_by, triggered_at, acknowledged_at, acknowledged_by,
		       resolved_at, resolved_by, muted_until, mute_reason, runbook_url, created_at
		FROM alerts`, r.alertCondCol)
}

// GetAlerts returns alerts for a team, optionally filtered by status.
func (r *Repository) GetAlerts(teamID int64, status string) ([]map[string]any, error) {
	query := r.selectSQL() + ` WHERE team_id = ?`
	args := []any{teamID}
	if status != "" {
		query += ` AND LOWER(status) = LOWER(?)`
		args = append(args, status)
	}
	query += ` ORDER BY created_at DESC`
	return dbutil.QueryMaps(r.db, query, args...)
}

// GetAlertsPaged returns paginated alerts.
func (r *Repository) GetAlertsPaged(teamID int64, limit, offset int) ([]map[string]any, int64, error) {
	query := r.selectSQL() + `
		WHERE team_id = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?`
	rows, err := dbutil.QueryMaps(r.db, query, teamID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM alerts WHERE team_id = ?`, teamID)
	return rows, total, nil
}

// GetAlertByID returns a single alert by its primary key.
func (r *Repository) GetAlertByID(id int64) (map[string]any, error) {
	return dbutil.QueryMap(r.db, r.selectSQL()+` WHERE id = ?`, id)
}

// CreateAlert creates a new alert rule and returns its ID.
func (r *Repository) CreateAlert(orgID, teamID int64, req types.AlertRequest) (int64, error) {
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

// MuteAlertWithReason applies a mute status and reason to an alert.
func (r *Repository) MuteAlertWithReason(id int64, minutes int, reason string) error {
	_, err := r.db.Exec(`
		UPDATE alerts
		SET status = 'muted', muted_until = ?, mute_reason = ?, updated_at = ?
		WHERE id = ?
	`, time.Now().UTC().Add(time.Duration(minutes)*time.Minute), reason, time.Now().UTC(), id)
	return err
}

// BulkMuteAlerts mutes multiple alerts.
func (r *Repository) BulkMuteAlerts(ids []int64, minutes int, reason string) ([]map[string]any, error) {
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

	return dbutil.QueryMaps(r.db, r.selectSQL()+` WHERE id IN `+inClause, inArgs...)
}

// BulkResolveAlerts resolves multiple alerts.
func (r *Repository) BulkResolveAlerts(ids []int64, resolvedBy string) ([]map[string]any, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	now := time.Now().UTC()

	inClause, inArgs := dbutil.InClauseInt64(ids)
	updateArgs := append([]any{now, resolvedBy, now}, inArgs...)
	if _, err := r.db.Exec(`UPDATE alerts SET status = 'resolved', resolved_at = ?, resolved_by = ?, updated_at = ? WHERE id IN `+inClause, updateArgs...); err != nil {
		return nil, err
	}

	return dbutil.QueryMaps(r.db, r.selectSQL()+` WHERE id IN `+inClause, inArgs...)
}

// GetAlertsForIncident returns alerts linked to a specific incident policy.
func (r *Repository) GetAlertsForIncident(teamID int64, policyID string) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, r.selectSQL()+`
		WHERE team_id = ? AND triggered_by LIKE ?
		ORDER BY created_at DESC
	`, teamID, "%"+policyID+"%")
}

// CountActiveAlerts returns the number of currently active alerts.
func (r *Repository) CountActiveAlerts(teamID int64) int64 {
	return dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM alerts WHERE team_id = ? AND status = 'active'`, teamID)
}

// GetIncidents returns a paginated list of incidents built from the alerts table.
func (r *Repository) GetIncidents(teamID int64, startMs, endMs int64, statuses, severities, services []string, limit, offset int) ([]map[string]any, int64, map[string]int64, map[string]int64, error) {
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

	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) `+queryBase, baseArgs...)

	allRows, _ := dbutil.QueryMaps(r.db, `SELECT status, severity `+queryBase, baseArgs...)
	statusCounts := map[string]int64{}
	severityCounts := map[string]int64{}
	for _, row := range allRows {
		status := dbutil.StringFromAny(row["status"])
		statusCounts[status]++
		severityCounts[dbutil.StringFromAny(row["severity"])]++
	}

	return items, total, statusCounts, severityCounts, nil
}

// UpdateAlertState applies a state transition to a single alert.
func (r *Repository) UpdateAlertState(id int64, status string, acknowledge, resolve, mute bool, user string, muteMinutes int) error {
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
