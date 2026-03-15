package alerting

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

func (r *Repository) CreateChannel(teamID, userID int64, req CreateChannelRequest) (*NotificationChannel, error) {
	result, err := r.db.Exec(`
		INSERT INTO notification_channels (team_id, created_by, name, channel_type, config, enabled)
		VALUES (?, ?, ?, ?, ?, true)
	`, teamID, userID, req.Name, req.ChannelType, req.Config)
	if err != nil {
		return nil, err
	}
	id, _ := result.LastInsertId()
	return r.GetChannelByID(teamID, id)
}

func (r *Repository) GetChannelByID(teamID, id int64) (*NotificationChannel, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, team_id, created_by, name, channel_type, config, enabled, created_at, updated_at
		FROM notification_channels WHERE id = ? AND team_id = ?
	`, id, teamID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return channelFromRow(row), nil
}

func (r *Repository) ListChannels(teamID int64) ([]NotificationChannel, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT id, team_id, created_by, name, channel_type, config, enabled, created_at, updated_at
		FROM notification_channels WHERE team_id = ? ORDER BY updated_at DESC
	`, teamID)
	if err != nil {
		return nil, err
	}
	channels := make([]NotificationChannel, 0, len(rows))
	for _, row := range rows {
		channels = append(channels, *channelFromRow(row))
	}
	return channels, nil
}

func (r *Repository) UpdateChannel(teamID, id int64, req UpdateChannelRequest) (*NotificationChannel, error) {
	setParts := []string{}
	args := []any{}
	if req.Name != nil {
		setParts = append(setParts, "name = ?")
		args = append(args, *req.Name)
	}
	if req.Config != nil {
		setParts = append(setParts, "config = ?")
		args = append(args, *req.Config)
	}
	if req.Enabled != nil {
		setParts = append(setParts, "enabled = ?")
		args = append(args, *req.Enabled)
	}
	if len(setParts) == 0 {
		return r.GetChannelByID(teamID, id)
	}

	query := "UPDATE notification_channels SET "
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
	return r.GetChannelByID(teamID, id)
}

func (r *Repository) DeleteChannel(teamID, id int64) error {
	result, err := r.db.Exec(`DELETE FROM notification_channels WHERE id = ? AND team_id = ?`, id, teamID)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("notification channel not found")
	}
	return nil
}

func channelFromRow(row map[string]any) *NotificationChannel {
	return &NotificationChannel{
		ID:          dbutil.Int64FromAny(row["id"]),
		TeamID:      dbutil.Int64FromAny(row["team_id"]),
		CreatedBy:   dbutil.Int64FromAny(row["created_by"]),
		Name:        dbutil.StringFromAny(row["name"]),
		ChannelType: dbutil.StringFromAny(row["channel_type"]),
		Config:      dbutil.StringFromAny(row["config"]),
		Enabled:     dbutil.BoolFromAny(row["enabled"]),
		CreatedAt:   dbutil.TimeFromAny(row["created_at"]),
		UpdatedAt:   dbutil.TimeFromAny(row["updated_at"]),
	}
}
