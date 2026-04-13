package team

import (
	"context"
	"database/sql"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	usershared "github.com/Optikk-Org/optikk-backend/internal/modules/user/internal/shared"
	"github.com/jmoiron/sqlx"
)

type Repository interface {
	FindUserByID(userID int64) (usershared.UserRecord, error)
	FindActiveUserByID(userID int64) (usershared.UserRecord, error)
	FindTeamByID(teamID int64) (usershared.TeamRecord, error)
	FindTeamBySlug(orgName, slug string) (usershared.TeamRecord, error)
	FindTeamByOrgAndName(orgName, teamName string) (usershared.TeamRecord, error)
	ListActiveTeamsByOrganization(orgName string) ([]usershared.TeamRecord, error)
	ListActiveTeamsByIDs(teamIDs []int64) ([]usershared.TeamRecord, error)
	UpdateUserTeams(userID int64, teamsJSON string) error
	CreateTeam(orgName, name, slug string, description *string, color, apiKey string, createdAt time.Time) (int64, error)
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
	err := r.db.GetContext(context.Background(), &u, `
		SELECT id, email, name, avatar_url, teams, active, last_login_at, created_at
		FROM users
		WHERE id = ?
		LIMIT 1
	`, userID)
	return u, err
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

func (r *MySQLRepository) FindTeamByID(teamID int64) (usershared.TeamRecord, error) {
	var t usershared.TeamRecord
	err := r.db.GetContext(context.Background(), &t, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE id = ?
		LIMIT 1
	`, teamID)
	return t, err
}

func (r *MySQLRepository) FindTeamBySlug(orgName, slug string) (usershared.TeamRecord, error) {
	var t usershared.TeamRecord
	err := r.db.GetContext(context.Background(), &t, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE org_name = ? AND slug = ?
		LIMIT 1
	`, orgName, slug)
	return t, err
}

func (r *MySQLRepository) FindTeamByOrgAndName(orgName, teamName string) (usershared.TeamRecord, error) {
	var t usershared.TeamRecord
	err := r.db.GetContext(context.Background(), &t, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE org_name = ? AND name = ? AND active = 1
		LIMIT 1
	`, orgName, teamName)
	return t, err
}

func (r *MySQLRepository) ListActiveTeamsByOrganization(orgName string) ([]usershared.TeamRecord, error) {
	var records []usershared.TeamRecord
	err := r.db.SelectContext(context.Background(), &records, `
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
	if err := r.db.SelectContext(context.Background(), &records, query, args...); err != nil {
		return nil, err
	}
	return records, nil
}

func (r *MySQLRepository) UpdateUserTeams(userID int64, teamsJSON string) error {
	_, err := r.db.ExecContext(context.Background(),
		`UPDATE users SET teams = ? WHERE id = ?`, teamsJSON, userID)
	return err
}

func (r *MySQLRepository) CreateTeam(orgName, name, slug string, description *string, color, apiKey string, createdAt time.Time) (int64, error) {
	res, err := r.db.ExecContext(context.Background(), `
		INSERT INTO teams (org_name, name, slug, description, active, color, api_key, created_at)
		VALUES (?, ?, ?, ?, 1, ?, ?, ?)
	`, orgName, name, slug, description, color, apiKey, createdAt)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}
