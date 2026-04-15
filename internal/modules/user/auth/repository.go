package auth

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	usershared "github.com/Optikk-Org/optikk-backend/internal/modules/user/internal/shared"
	"github.com/jmoiron/sqlx"
)

type Repository interface {
	FindActiveUserByID(userID int64) (usershared.UserRecord, error)
	FindActiveUserByEmail(email string) (usershared.AuthUser, error)
	ListActiveTeamsByIDs(teamIDs []int64) ([]usershared.TeamRecord, error)
	UpdateUserLastLogin(userID int64, at time.Time) error
}

type MySQLRepository struct {
	db *sqlx.DB
}

func NewRepository(db *sql.DB, appConfig registry.AppConfig) *MySQLRepository {
	return &MySQLRepository{
		db: sqlx.NewDb(db, "mysql"),
	}
}

func (r *MySQLRepository) FindActiveUserByID(userID int64) (usershared.UserRecord, error) {
	var u usershared.UserRecord
	err := r.db.GetContext(context.Background(), &u, `
		SELECT id, email, name, avatar_url, teams, active, last_login_at, created_at
		FROM users
		WHERE id = ? AND active = 1
		LIMIT 1
	`, userID)
	return u, err
}

func (r *MySQLRepository) FindActiveUserByEmail(email string) (usershared.AuthUser, error) {
	var u usershared.AuthUser
	err := r.db.GetContext(context.Background(), &u, `
		SELECT id, email, password_hash, name, avatar_url, teams
		FROM users
		WHERE email = ? AND active = 1
		LIMIT 1
	`, strings.TrimSpace(email))
	return u, err
}

func (r *MySQLRepository) ListActiveTeamsByIDs(teamIDs []int64) ([]usershared.TeamRecord, error) {
	if len(teamIDs) == 0 {
		return []usershared.TeamRecord{}, nil
	}
	query, args, err := sqlx.In(`
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE id IN (?) AND active = 1
		ORDER BY created_at DESC
	`, teamIDs)
	if err != nil {
		return nil, err
	}
	query = r.db.Rebind(query)
	var records []usershared.TeamRecord
	if err := r.db.SelectContext(context.Background(), &records, query, args...); err != nil {
		return nil, err
	}
	return records, nil
}

func (r *MySQLRepository) UpdateUserLastLogin(userID int64, at time.Time) error {
	_, err := r.db.ExecContext(context.Background(),
		`UPDATE users SET last_login_at = ? WHERE id = ?`, at, userID)
	return err
}
