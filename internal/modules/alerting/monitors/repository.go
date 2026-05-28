package monitors

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
	"github.com/jmoiron/sqlx"
)

// Repository owns every SQL touch for monitors + monitor_state CRUD reads.
// Mutations that move state (ack/mute/test/transition) live in repository_state.go
// once the evaluator lands. Phase 1 ships CRUD only.
type Repository interface {
	Create(ctx context.Context, row insertArgs) (int64, error)
	Update(ctx context.Context, id, teamID int64, row insertArgs) error
	Delete(ctx context.Context, id, teamID int64) error
	GetByID(ctx context.Context, id, teamID int64) (models.MonitorRow, models.MonitorStateRow, error)
	List(ctx context.Context, teamID int64, q ListQuery) ([]models.MonitorRow, []models.MonitorStateRow, error)
}

type MySQLRepository struct {
	db *sqlx.DB
}

func NewRepository(db *sql.DB) *MySQLRepository {
	return &MySQLRepository{db: sqlx.NewDb(db, "mysql")}
}

// insertArgs bundles the column values for INSERT/UPDATE. Encoding to JSON
// happens in service.go; the repo just binds bytes verbatim.
type insertArgs struct {
	TeamID            int64
	Name              string
	Type              string
	Priority          string
	ScopeJSON         []byte
	QueryJSON         []byte
	ConditionsJSON    []byte
	NotifyJSON        []byte
	MessageBody       sql.NullString
	RunbookURL        sql.NullString
	TagsJSON          []byte
	EvalEverySec      int
	RenotifyEverySec  sql.NullInt64
	CreatedByUserID   sql.NullInt64
}

const insertMonitor = `
INSERT INTO observability.monitors
  (team_id, name, type, priority, scope_json, query_json, conditions_json, notify_json,
   message_body, runbook_url, tags_json, eval_every_sec, renotify_every_sec,
   active, created_at, created_by_user_id)
VALUES
  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
`

const insertInitialState = `
INSERT INTO observability.monitor_state
  (monitor_id, status, next_evaluation_at)
VALUES
  (?, 'no_data', ?)
`

func (r *MySQLRepository) Create(ctx context.Context, row insertArgs) (int64, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, insertMonitor,
		row.TeamID, row.Name, row.Type, row.Priority,
		row.ScopeJSON, row.QueryJSON, row.ConditionsJSON, row.NotifyJSON,
		row.MessageBody, row.RunbookURL, row.TagsJSON,
		row.EvalEverySec, row.RenotifyEverySec,
		now, row.CreatedByUserID)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, insertInitialState, id, now); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

const updateMonitor = `
UPDATE observability.monitors
   SET name = ?, type = ?, priority = ?,
       scope_json = ?, query_json = ?, conditions_json = ?, notify_json = ?,
       message_body = ?, runbook_url = ?, tags_json = ?,
       eval_every_sec = ?, renotify_every_sec = ?,
       updated_at = ?
 WHERE id = ? AND team_id = ?
`

func (r *MySQLRepository) Update(ctx context.Context, id, teamID int64, row insertArgs) error {
	res, err := dbutil.ExecSQL(ctx, r.db, "monitors.Update", updateMonitor,
		row.Name, row.Type, row.Priority,
		row.ScopeJSON, row.QueryJSON, row.ConditionsJSON, row.NotifyJSON,
		row.MessageBody, row.RunbookURL, row.TagsJSON,
		row.EvalEverySec, row.RenotifyEverySec,
		time.Now().UTC(), id, teamID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (r *MySQLRepository) Delete(ctx context.Context, id, teamID int64) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx,
		`DELETE FROM observability.monitors WHERE id = ? AND team_id = ?`, id, teamID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM observability.monitor_state WHERE monitor_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM observability.monitor_events WHERE monitor_id = ?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

const selectMonitorCols = `
  m.id, m.team_id, m.name, m.type, m.priority,
  m.scope_json, m.query_json, m.conditions_json, m.notify_json,
  m.message_template_id, m.message_body, m.runbook_url, m.tags_json,
  m.eval_every_sec, m.renotify_every_sec, m.muted_until, m.active,
  m.created_at, m.updated_at, m.created_by_user_id
`

const selectStateCols = `
  s.monitor_id, s.status, s.current_value, s.last_evaluated_at,
  s.next_evaluation_at, s.triggered_at, s.last_notified_at,
  s.evaluation_count, s.acked_by_user_id, s.acked_at
`

func (r *MySQLRepository) GetByID(ctx context.Context, id, teamID int64) (models.MonitorRow, models.MonitorStateRow, error) {
	var row models.MonitorRow
	var state models.MonitorStateRow
	q := fmt.Sprintf(`
		SELECT %s, %s
		  FROM observability.monitors m
		  LEFT JOIN observability.monitor_state s ON s.monitor_id = m.id
		 WHERE m.id = ? AND m.team_id = ?
		 LIMIT 1
	`, selectMonitorCols, selectStateCols)
	var combined monitorWithState
	if err := dbutil.GetSQL(ctx, r.db, "monitors.GetByID", &combined, q, id, teamID); err != nil {
		return row, state, err
	}
	return combined.toRows()
}

// monitorWithState flattens the SELECT m.* + s.* row so sqlx can scan it.
type monitorWithState struct {
	models.MonitorRow
	StateMonitorID        sql.NullInt64   `db:"monitor_id"`
	StateStatus           sql.NullString  `db:"status"`
	StateCurrentValue     sql.NullFloat64 `db:"current_value"`
	StateLastEvaluatedAt  sql.NullTime    `db:"last_evaluated_at"`
	StateNextEvaluationAt sql.NullTime    `db:"next_evaluation_at"`
	StateTriggeredAt      sql.NullTime    `db:"triggered_at"`
	StateLastNotifiedAt   sql.NullTime    `db:"last_notified_at"`
	StateEvaluationCount  sql.NullInt64   `db:"evaluation_count"`
	StateAckedByUserID    sql.NullInt64   `db:"acked_by_user_id"`
	StateAckedAt          sql.NullTime    `db:"acked_at"`
}

func (m monitorWithState) toRows() (models.MonitorRow, models.MonitorStateRow, error) {
	state := models.MonitorStateRow{}
	if m.StateMonitorID.Valid {
		state.MonitorID = m.StateMonitorID.Int64
		state.Status = m.StateStatus.String
		state.CurrentValue = m.StateCurrentValue
		state.LastEvaluatedAt = m.StateLastEvaluatedAt
		if m.StateNextEvaluationAt.Valid {
			state.NextEvaluationAt = m.StateNextEvaluationAt.Time
		}
		state.TriggeredAt = m.StateTriggeredAt
		state.LastNotifiedAt = m.StateLastNotifiedAt
		state.EvaluationCount = m.StateEvaluationCount.Int64
		state.AckedByUserID = m.StateAckedByUserID
		state.AckedAt = m.StateAckedAt
	}
	return m.MonitorRow, state, nil
}

func (r *MySQLRepository) List(ctx context.Context, teamID int64, q ListQuery) ([]models.MonitorRow, []models.MonitorStateRow, error) {
	where := []string{"m.team_id = ?"}
	args := []any{teamID}
	if q.Type != "" {
		where = append(where, "m.type = ?")
		args = append(args, q.Type)
	}
	if q.Priority != "" {
		where = append(where, "m.priority = ?")
		args = append(args, q.Priority)
	}
	if q.Status != "" {
		where = append(where, "s.status = ?")
		args = append(args, q.Status)
	}
	if q.Muted != nil {
		if *q.Muted {
			where = append(where, "m.muted_until IS NOT NULL AND m.muted_until > NOW()")
		} else {
			where = append(where, "(m.muted_until IS NULL OR m.muted_until <= NOW())")
		}
	}
	if q.Search != "" {
		where = append(where, "m.name LIKE ?")
		args = append(args, "%"+q.Search+"%")
	}
	limit := q.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset := q.Offset
	if offset < 0 {
		offset = 0
	}
	args = append(args, limit, offset)

	query := fmt.Sprintf(`
		SELECT %s, %s
		  FROM observability.monitors m
		  LEFT JOIN observability.monitor_state s ON s.monitor_id = m.id
		 WHERE %s
		 ORDER BY m.created_at DESC
		 LIMIT ? OFFSET ?
	`, selectMonitorCols, selectStateCols, strings.Join(where, " AND "))

	var combined []monitorWithState
	if err := dbutil.SelectSQL(ctx, r.db, "monitors.List", &combined, query, args...); err != nil {
		return nil, nil, err
	}
	rows := make([]models.MonitorRow, 0, len(combined))
	states := make([]models.MonitorStateRow, 0, len(combined))
	for _, m := range combined {
		row, state, _ := m.toRows()
		rows = append(rows, row)
		states = append(states, state)
	}
	return rows, states, nil
}
