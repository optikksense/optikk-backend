package errorsinbox

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type Repository struct {
	db dbutil.Querier
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: dbutil.NewMySQLWrapper(db)}
}

func (r *Repository) GetByGroupID(teamID int64, groupID string) (*ErrorGroupStatus, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, team_id, group_id, status, assigned_to, snoozed_until, note, updated_by, created_at, updated_at
		FROM error_group_status WHERE team_id = ? AND group_id = ?
	`, teamID, groupID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return statusFromRow(row), nil
}

func (r *Repository) ListByStatus(teamID int64, status string, limit int) ([]ErrorGroupStatus, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT id, team_id, group_id, status, assigned_to, snoozed_until, note, updated_by, created_at, updated_at
		FROM error_group_status WHERE team_id = ? AND status = ?
		ORDER BY updated_at DESC LIMIT ?
	`, teamID, status, limit)
	if err != nil {
		return nil, err
	}
	result := make([]ErrorGroupStatus, 0, len(rows))
	for _, row := range rows {
		result = append(result, *statusFromRow(row))
	}
	return result, nil
}

func (r *Repository) Upsert(teamID int64, groupID, status string, assignedTo *int64, snoozedUntil *time.Time, note string, updatedBy int64) (*ErrorGroupStatus, error) {
	_, err := r.db.Exec(`
		INSERT INTO error_group_status (team_id, group_id, status, assigned_to, snoozed_until, note, updated_by)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE status = VALUES(status), assigned_to = VALUES(assigned_to),
			snoozed_until = VALUES(snoozed_until), note = VALUES(note), updated_by = VALUES(updated_by)
	`, teamID, groupID, status, assignedTo, snoozedUntil, note, updatedBy)
	if err != nil {
		return nil, err
	}
	return r.GetByGroupID(teamID, groupID)
}

func (r *Repository) BulkUpsert(teamID int64, groupIDs []string, status string, assignedTo *int64, note string, updatedBy int64) error {
	if len(groupIDs) == 0 {
		return nil
	}
	valueParts := make([]string, 0, len(groupIDs))
	args := make([]any, 0, len(groupIDs)*6)
	for _, gid := range groupIDs {
		valueParts = append(valueParts, "(?, ?, ?, ?, ?, ?)")
		args = append(args, teamID, gid, status, assignedTo, note, updatedBy)
	}
	query := fmt.Sprintf(`
		INSERT INTO error_group_status (team_id, group_id, status, assigned_to, note, updated_by)
		VALUES %s
		ON DUPLICATE KEY UPDATE status = VALUES(status), assigned_to = VALUES(assigned_to),
			note = VALUES(note), updated_by = VALUES(updated_by)
	`, strings.Join(valueParts, ", "))
	_, err := r.db.Exec(query, args...)
	return err
}

func (r *Repository) CountByStatus(teamID int64) (map[string]int64, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT status, COUNT(*) as cnt FROM error_group_status
		WHERE team_id = ? GROUP BY status
	`, teamID)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int64)
	for _, row := range rows {
		counts[dbutil.StringFromAny(row["status"])] = dbutil.Int64FromAny(row["cnt"])
	}
	return counts, nil
}

func statusFromRow(row map[string]any) *ErrorGroupStatus {
	s := &ErrorGroupStatus{
		ID:        dbutil.Int64FromAny(row["id"]),
		TeamID:    dbutil.Int64FromAny(row["team_id"]),
		GroupID:   dbutil.StringFromAny(row["group_id"]),
		Status:    dbutil.StringFromAny(row["status"]),
		Note:      dbutil.StringFromAny(row["note"]),
		UpdatedBy: dbutil.Int64FromAny(row["updated_by"]),
		CreatedAt: dbutil.TimeFromAny(row["created_at"]),
		UpdatedAt: dbutil.TimeFromAny(row["updated_at"]),
	}
	if v := row["assigned_to"]; v != nil {
		val := dbutil.Int64FromAny(v)
		if val != 0 {
			s.AssignedTo = &val
		}
	}
	if t := dbutil.NullableTimeFromAny(row["snoozed_until"]); t != nil {
		s.SnoozedUntil = t
	}
	return s
}
