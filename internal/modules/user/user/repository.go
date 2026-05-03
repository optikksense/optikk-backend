package userpage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	usershared "github.com/Optikk-Org/optikk-backend/internal/modules/user/internal/shared"
	"github.com/jmoiron/sqlx"
)

type Repository interface {
	FindUserByID(userID int64) (usershared.UserRecord, error)
	FindActiveUserByID(userID int64) (usershared.UserRecord, error)
	FindTeamByID(teamID int64) (usershared.TeamRecord, error)
	ListActiveTeamsByOrganization(orgName string) ([]usershared.TeamRecord, error)
	ListActiveTeamsByIDs(teamIDs []int64) ([]usershared.TeamRecord, error)
	ListActiveUsersByTeamIDs(teamIDs []int64, limit, offset int) ([]usershared.UserRecord, error)
	CreateUser(email, passwordHash, name string, avatarURL, teamsJSON *string, createdAt time.Time) (int64, error)
	UpdateUserProfile(userID int64, name, avatarURL *string) error
}

type MySQLRepository struct {
	db *sqlx.DB
}

func NewRepository(db *sql.DB, appConfig registry.AppConfig) *MySQLRepository {
	return &MySQLRepository{
		db: sqlx.NewDb(db, "mysql"),
	}
}

func (r *MySQLRepository) FindUserByID(userID int64) (usershared.UserRecord, error) {
	var u usershared.UserRecord
	err := dbutil.GetSQL(context.Background(), r.db, "user.FindUserByID", &u, `
		SELECT id, email, name, avatar_url, teams, active, last_login_at, created_at
		FROM users
		WHERE id = ?
		LIMIT 1
	`, userID)
	return u, err
}

func (r *MySQLRepository) FindActiveUserByID(userID int64) (usershared.UserRecord, error) {
	var u usershared.UserRecord
	err := dbutil.GetSQL(context.Background(), r.db, "user.FindActiveUserByID", &u, `
		SELECT id, email, name, avatar_url, teams, active, last_login_at, created_at
		FROM users
		WHERE id = ? AND active = 1
		LIMIT 1
	`, userID)
	return u, err
}

func (r *MySQLRepository) FindTeamByID(teamID int64) (usershared.TeamRecord, error) {
	var t usershared.TeamRecord
	err := dbutil.GetSQL(context.Background(), r.db, "user.FindTeamByID", &t, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE id = ?
		LIMIT 1
	`, teamID)
	return t, err
}

func (r *MySQLRepository) ListActiveTeamsByOrganization(orgName string) ([]usershared.TeamRecord, error) {
	var records []usershared.TeamRecord
	err := dbutil.SelectSQL(context.Background(), r.db, "user.ListActiveTeamsByOrganization", &records, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE org_name = ? AND active = 1
		ORDER BY created_at DESC
	`, orgName)
	return records, err
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
	if err := dbutil.SelectSQL(context.Background(), r.db, "user.ListActiveTeamsByIDs", &records, query, args...); err != nil {
		return nil, err
	}
	return records, nil
}

func (r *MySQLRepository) ListActiveUsersByTeamIDs(teamIDs []int64, limit, offset int) ([]usershared.UserRecord, error) {
	if len(teamIDs) == 0 {
		return []usershared.UserRecord{}, nil
	}

	conditions := make([]string, 0, len(teamIDs))
	args := make([]any, 0, len(teamIDs)+2)
	for _, teamID := range teamIDs {
		conditions = append(conditions, `JSON_CONTAINS(teams, ?)`)
		args = append(args, fmt.Sprintf(`{"team_id":%d}`, teamID))
	}
	args = append(args, limit, offset)

	var records []usershared.UserRecord
	err := dbutil.SelectSQL(context.Background(), r.db, "user.ListActiveUsersByTeamIDs", &records, fmt.Sprintf(`
		SELECT id, email, name, avatar_url, teams, active, last_login_at, created_at
		FROM users
		WHERE (%s) AND active = 1
		ORDER BY id
		LIMIT ? OFFSET ?
	`, strings.Join(conditions, " OR ")), args...)
	return records, err
}

func (r *MySQLRepository) CreateUser(email, passwordHash, name string, avatarURL, teamsJSON *string, createdAt time.Time) (int64, error) {
	res, err := dbutil.ExecSQL(context.Background(), r.db, "user.CreateUser", `
		INSERT INTO users (email, password_hash, name, avatar_url, teams, active, created_at)
		VALUES (?, ?, ?, ?, ?, 1, ?)
	`, email, usershared.NullableString(passwordHash), name, avatarURL, teamsJSON, createdAt)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (r *MySQLRepository) UpdateUserProfile(userID int64, name, avatarURL *string) error {
	_, err := dbutil.ExecSQL(context.Background(), r.db, "user.UpdateUserProfile", `
		UPDATE users
		SET name = COALESCE(?, name), avatar_url = COALESCE(?, avatar_url)
		WHERE id = ?
	`, name, avatarURL, userID)
	return err
}
