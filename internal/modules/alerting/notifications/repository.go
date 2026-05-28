package notifications

import (
	"context"
	"database/sql"
	"time"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
	"github.com/jmoiron/sqlx"
)

// Repository owns every SQL touch across channels, policies, and templates.
// Channel-usage counts are computed inline by the service via CountChannelUsage.
type Repository interface {
	CreateChannel(ctx context.Context, row models.ChannelRow) (int64, error)
	UpdateChannel(ctx context.Context, id, teamID int64, row models.ChannelRow) error
	DeleteChannel(ctx context.Context, id, teamID int64) error
	GetChannel(ctx context.Context, id, teamID int64) (models.ChannelRow, error)
	ListChannels(ctx context.Context, teamID int64) ([]models.ChannelRow, error)
	CountChannelUsage(ctx context.Context, teamID int64) (map[int64]int, error)
	MarkChannelDelivered(ctx context.Context, id int64, at time.Time, errText sql.NullString) error

	CreatePolicy(ctx context.Context, row models.PolicyRow) (int64, error)
	UpdatePolicy(ctx context.Context, id, teamID int64, row models.PolicyRow) error
	DeletePolicy(ctx context.Context, id, teamID int64) error
	ListPolicies(ctx context.Context, teamID int64) ([]models.PolicyRow, error)

	CreateTemplate(ctx context.Context, row models.TemplateRow) (int64, error)
	UpdateTemplate(ctx context.Context, id, teamID int64, row models.TemplateRow) error
	DeleteTemplate(ctx context.Context, id, teamID int64) error
	ListTemplates(ctx context.Context, teamID int64) ([]models.TemplateRow, error)
}

type MySQLRepository struct {
	db *sqlx.DB
}

func NewRepository(db *sql.DB) *MySQLRepository {
	return &MySQLRepository{db: sqlx.NewDb(db, "mysql")}
}

// Channels ------------------------------------------------------------------

func (r *MySQLRepository) CreateChannel(ctx context.Context, row models.ChannelRow) (int64, error) {
	res, err := dbutil.ExecSQL(ctx, r.db, "notifications.CreateChannel", `
		INSERT INTO observability.notification_channels
		  (team_id, type, name, config_json, status, created_at)
		VALUES (?, ?, ?, ?, 'ok', ?)
	`, row.TeamID, row.Type, row.Name, row.ConfigJSON, time.Now().UTC())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (r *MySQLRepository) UpdateChannel(ctx context.Context, id, teamID int64, row models.ChannelRow) error {
	res, err := dbutil.ExecSQL(ctx, r.db, "notifications.UpdateChannel", `
		UPDATE observability.notification_channels
		   SET type = ?, name = ?, config_json = ?, updated_at = ?
		 WHERE id = ? AND team_id = ?
	`, row.Type, row.Name, row.ConfigJSON, time.Now().UTC(), id, teamID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (r *MySQLRepository) DeleteChannel(ctx context.Context, id, teamID int64) error {
	res, err := dbutil.ExecSQL(ctx, r.db, "notifications.DeleteChannel",
		`DELETE FROM observability.notification_channels WHERE id = ? AND team_id = ?`, id, teamID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

const channelCols = `id, team_id, type, name, config_json, status,
  last_used_at, last_delivery_at, last_error_text, created_at, updated_at`

func (r *MySQLRepository) GetChannel(ctx context.Context, id, teamID int64) (models.ChannelRow, error) {
	var row models.ChannelRow
	err := dbutil.GetSQL(ctx, r.db, "notifications.GetChannel", &row,
		`SELECT `+channelCols+` FROM observability.notification_channels WHERE id = ? AND team_id = ? LIMIT 1`,
		id, teamID)
	return row, err
}

func (r *MySQLRepository) ListChannels(ctx context.Context, teamID int64) ([]models.ChannelRow, error) {
	var rows []models.ChannelRow
	err := dbutil.SelectSQL(ctx, r.db, "notifications.ListChannels", &rows,
		`SELECT `+channelCols+` FROM observability.notification_channels WHERE team_id = ? ORDER BY created_at DESC`,
		teamID)
	return rows, err
}

func (r *MySQLRepository) CountChannelUsage(ctx context.Context, teamID int64) (map[int64]int, error) {
	rows, err := r.db.QueryxContext(ctx, `
		SELECT j.channel_id AS cid, COUNT(*) AS cnt
		  FROM observability.monitors m
		  JOIN JSON_TABLE(m.notify_json, '$.channel_ids[*]'
		         COLUMNS (channel_id BIGINT PATH '$')) AS j
		 WHERE m.team_id = ?
		 GROUP BY j.channel_id
	`, teamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]int{}
	for rows.Next() {
		var cid int64
		var cnt int
		if err := rows.Scan(&cid, &cnt); err != nil {
			return nil, err
		}
		out[cid] = cnt
	}
	return out, rows.Err()
}

func (r *MySQLRepository) MarkChannelDelivered(ctx context.Context, id int64, at time.Time, errText sql.NullString) error {
	status := "ok"
	if errText.Valid && errText.String != "" {
		status = "warn"
	}
	_, err := dbutil.ExecSQL(ctx, r.db, "notifications.MarkChannelDelivered", `
		UPDATE observability.notification_channels
		   SET last_used_at = ?, last_delivery_at = ?, last_error_text = ?, status = ?
		 WHERE id = ?
	`, at, at, errText, status, id)
	return err
}

// Policies ------------------------------------------------------------------

const policyCols = `id, team_id, name, match_dsl, actions_json, hits_30d,
  last_used_at, enabled, position, created_at, updated_at`

func (r *MySQLRepository) CreatePolicy(ctx context.Context, row models.PolicyRow) (int64, error) {
	res, err := dbutil.ExecSQL(ctx, r.db, "notifications.CreatePolicy", `
		INSERT INTO observability.notification_policies
		  (team_id, name, match_dsl, actions_json, enabled, position, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, row.TeamID, row.Name, row.MatchDSL, row.ActionsJSON, row.Enabled, row.Position, time.Now().UTC())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (r *MySQLRepository) UpdatePolicy(ctx context.Context, id, teamID int64, row models.PolicyRow) error {
	res, err := dbutil.ExecSQL(ctx, r.db, "notifications.UpdatePolicy", `
		UPDATE observability.notification_policies
		   SET name = ?, match_dsl = ?, actions_json = ?, enabled = ?, position = ?, updated_at = ?
		 WHERE id = ? AND team_id = ?
	`, row.Name, row.MatchDSL, row.ActionsJSON, row.Enabled, row.Position, time.Now().UTC(), id, teamID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (r *MySQLRepository) DeletePolicy(ctx context.Context, id, teamID int64) error {
	res, err := dbutil.ExecSQL(ctx, r.db, "notifications.DeletePolicy",
		`DELETE FROM observability.notification_policies WHERE id = ? AND team_id = ?`, id, teamID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (r *MySQLRepository) ListPolicies(ctx context.Context, teamID int64) ([]models.PolicyRow, error) {
	var rows []models.PolicyRow
	err := dbutil.SelectSQL(ctx, r.db, "notifications.ListPolicies", &rows,
		`SELECT `+policyCols+` FROM observability.notification_policies WHERE team_id = ? ORDER BY position ASC, id ASC`,
		teamID)
	return rows, err
}

// Templates -----------------------------------------------------------------

const templateCols = `id, team_id, name, description, body, used_count, created_at, updated_at`

func (r *MySQLRepository) CreateTemplate(ctx context.Context, row models.TemplateRow) (int64, error) {
	res, err := dbutil.ExecSQL(ctx, r.db, "notifications.CreateTemplate", `
		INSERT INTO observability.notification_templates
		  (team_id, name, description, body, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, row.TeamID, row.Name, row.Description, row.Body, time.Now().UTC())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (r *MySQLRepository) UpdateTemplate(ctx context.Context, id, teamID int64, row models.TemplateRow) error {
	res, err := dbutil.ExecSQL(ctx, r.db, "notifications.UpdateTemplate", `
		UPDATE observability.notification_templates
		   SET name = ?, description = ?, body = ?, updated_at = ?
		 WHERE id = ? AND team_id = ?
	`, row.Name, row.Description, row.Body, time.Now().UTC(), id, teamID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (r *MySQLRepository) DeleteTemplate(ctx context.Context, id, teamID int64) error {
	res, err := dbutil.ExecSQL(ctx, r.db, "notifications.DeleteTemplate",
		`DELETE FROM observability.notification_templates WHERE id = ? AND team_id = ?`, id, teamID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (r *MySQLRepository) ListTemplates(ctx context.Context, teamID int64) ([]models.TemplateRow, error) {
	var rows []models.TemplateRow
	err := dbutil.SelectSQL(ctx, r.db, "notifications.ListTemplates", &rows,
		`SELECT `+templateCols+` FROM observability.notification_templates WHERE team_id = ? ORDER BY created_at DESC`,
		teamID)
	return rows, err
}

// ChannelInUse reports whether any monitor's notify_json references the channel.
// Used by service.DeleteChannel to return 409 when the channel is still bound.
func (r *MySQLRepository) ChannelInUse(ctx context.Context, channelID, teamID int64) (bool, error) {
	var cnt int
	err := dbutil.GetSQL(ctx, r.db, "notifications.ChannelInUse", &cnt, `
		SELECT COUNT(*)
		  FROM observability.monitors m
		  JOIN JSON_TABLE(m.notify_json, '$.channel_ids[*]'
		         COLUMNS (channel_id BIGINT PATH '$')) AS j
		 WHERE m.team_id = ? AND j.channel_id = ?
	`, teamID, channelID)
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}
