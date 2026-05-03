package savedviews

import (
	"context"
	"database/sql"
	"errors"
	"time"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/jmoiron/sqlx"
)

type Repository struct {
	db *sqlx.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: sqlx.NewDb(db, "mysql")}
}

type sqlRow struct {
	ID         int64        `db:"id"`
	TeamID     int64        `db:"team_id"`
	UserID     int64        `db:"user_id"`
	Scope      string       `db:"scope"`
	Name       string       `db:"name"`
	URL        string       `db:"url"`
	Visibility string       `db:"visibility"`
	CreatedAt  time.Time    `db:"created_at"`
	UpdatedAt  sql.NullTime `db:"updated_at"`
}

func (r sqlRow) toModel() SavedView {
	return SavedView{
		ID:         r.ID,
		TeamID:     r.TeamID,
		UserID:     r.UserID,
		Scope:      r.Scope,
		Name:       r.Name,
		URL:        r.URL,
		Visibility: r.Visibility,
		CreatedAt:  r.CreatedAt,
		UpdatedAt:  r.UpdatedAt.Time,
	}
}

func (r *Repository) List(ctx context.Context, teamID int64, scope string) ([]SavedView, error) {
	var rows []sqlRow
	const q = `
		SELECT id, team_id, user_id, scope, name, url, visibility, created_at, updated_at
		FROM saved_views
		WHERE team_id = ? AND (scope = ? OR ? = '')
		ORDER BY created_at DESC
		LIMIT 200
	`
	if err := dbutil.SelectSQL(ctx, r.db, "savedviews.List", &rows, q, teamID, scope, scope); err != nil {
		return nil, err
	}
	out := make([]SavedView, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.toModel())
	}
	return out, nil
}

func (r *Repository) Create(ctx context.Context, v SavedView) (SavedView, error) {
	const q = `
		INSERT INTO saved_views (team_id, user_id, scope, name, url, visibility, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`
	now := time.Now().UTC()
	res, err := dbutil.ExecSQL(ctx, r.db, "savedviews.Create", q,
		v.TeamID, v.UserID, v.Scope, v.Name, v.URL, v.Visibility, now)
	if err != nil {
		return SavedView{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return SavedView{}, err
	}
	v.ID = id
	v.CreatedAt = now
	return v, nil
}

func (r *Repository) Delete(ctx context.Context, teamID, userID, id int64) error {
	const q = `DELETE FROM saved_views WHERE id = ? AND team_id = ? AND user_id = ?`
	res, err := dbutil.ExecSQL(ctx, r.db, "savedviews.Delete", q, id, teamID, userID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("saved view not found")
	}
	return nil
}
