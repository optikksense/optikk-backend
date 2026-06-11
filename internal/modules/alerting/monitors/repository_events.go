package monitors

import (
	"context"
	"database/sql"
	"time"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// EventRow is the read shape for monitor_events plus the monitor name.
type EventRow struct {
	models.MonitorEventRow
	MonitorName string `db:"monitor_name"`
}

// Events returns the recent triggers for one monitor (paginated by limit).
func (r *Repository) Events(ctx context.Context, monitorID, teamID int64, limit int) ([]EventRow, error) {
	if limit <= 0 || limit > 200 {
		limit = 20
	}
	const q = `
		SELECT e.id, e.monitor_id, e.team_id, e.kind, e.value, e.threshold,
		       e.started_at, e.ended_at, e.resolved_by, e.peak_value, e.note,
		       m.name AS monitor_name
		  FROM observability.monitor_events e
		  JOIN observability.monitors m ON m.id = e.monitor_id
		 WHERE e.monitor_id = ? AND e.team_id = ?
		 ORDER BY e.started_at DESC
		 LIMIT ?
	`
	var rows []EventRow
	err := dbutil.SelectSQL(ctx, r.db, "monitors.Events", &rows, q, monitorID, teamID, limit)
	return rows, err
}

// Activity returns recent events across all monitors for the team — powers
// the list page's "Recent activity" card.
func (r *Repository) Activity(ctx context.Context, teamID int64, since time.Time, limit int) ([]EventRow, error) {
	if limit <= 0 || limit > 200 {
		limit = 20
	}
	const q = `
		SELECT e.id, e.monitor_id, e.team_id, e.kind, e.value, e.threshold,
		       e.started_at, e.ended_at, e.resolved_by, e.peak_value, e.note,
		       m.name AS monitor_name
		  FROM observability.monitor_events e
		  JOIN observability.monitors m ON m.id = e.monitor_id
		 WHERE e.team_id = ? AND e.started_at >= ?
		 ORDER BY e.started_at DESC
		 LIMIT ?
	`
	var rows []EventRow
	err := dbutil.SelectSQL(ctx, r.db, "monitors.Activity", &rows, q, teamID, since, limit)
	return rows, err
}

// StatusTimelineRows returns raw events to build the 24h status bands.
func (r *Repository) StatusTimelineRows(ctx context.Context, monitorID, teamID int64, since time.Time) ([]models.MonitorEventRow, error) {
	var rows []models.MonitorEventRow
	const q = `
		SELECT id, monitor_id, team_id, kind, value, threshold, started_at,
		       ended_at, resolved_by, peak_value, note
		  FROM observability.monitor_events
		 WHERE monitor_id = ? AND team_id = ? AND started_at >= ?
		 ORDER BY started_at ASC
	`
	err := dbutil.SelectSQL(ctx, r.db, "monitors.StatusTimelineRows", &rows, q, monitorID, teamID, since)
	return rows, err
}

// Ack marks the monitor's alerting state as acknowledged. Returns
// sql.ErrNoRows if not currently in an alerting/warning state.
func (r *Repository) Ack(ctx context.Context, monitorID, teamID, userID int64, at time.Time) error {
	const q = `
		UPDATE observability.monitor_state s
		   JOIN observability.monitors m ON m.id = s.monitor_id
		    SET s.acked_by_user_id = ?, s.acked_at = ?
		  WHERE s.monitor_id = ? AND m.team_id = ? AND s.status IN ('alert','warn')
	`
	res, err := dbutil.ExecSQL(ctx, r.db, "monitors.Ack", q, userID, at, monitorID, teamID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// Mute sets muted_until to (now + duration). 0 duration clears the mute.
func (r *Repository) Mute(ctx context.Context, monitorID, teamID int64, until sql.NullTime) error {
	const q = `UPDATE observability.monitors SET muted_until = ?, updated_at = ? WHERE id = ? AND team_id = ?`
	res, err := dbutil.ExecSQL(ctx, r.db, "monitors.Mute", q, until, time.Now().UTC(), monitorID, teamID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}
