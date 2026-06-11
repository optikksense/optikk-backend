package user

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/jmoiron/sqlx"
)

// Repository implements the *Repository interface for MySQL.
type Repository struct {
	db *sqlx.DB
}

// NewRepository creates a new Repository instance.
func NewRepository(db *sql.DB, appConfig registry.AppConfig) *Repository {
	return &Repository{
		db: sqlx.NewDb(db, "mysql"),
	}
}

// FindActiveUserByID loads an active user record by ID.
func (r *Repository) FindActiveUserByID(userID int64) (UserRecord, error) {
	var u UserRecord
	err := dbutil.GetSQL(context.Background(), r.db, "user.FindActiveUserByID", &u, `
		SELECT id, email, name, avatar_url, teams, active, last_login_at, created_at
		FROM users
		WHERE id = ? AND active = 1
		LIMIT 1
	`, userID)
	return u, err
}

// FindActiveUserByEmail loads an active user record by Email.
func (r *Repository) FindActiveUserByEmail(email string) (AuthUser, error) {
	var u AuthUser
	err := dbutil.GetSQL(context.Background(), r.db, "user.FindActiveUserByEmail", &u, `
		SELECT id, email, password_hash, name, avatar_url, teams
		FROM users
		WHERE email = ? AND active = 1
		LIMIT 1
	`, strings.TrimSpace(email))
	return u, err
}

// UpdateUserLastLogin updates the login timestamp for a user.
func (r *Repository) UpdateUserLastLogin(userID int64, at time.Time) error {
	_, err := dbutil.ExecSQL(context.Background(), r.db, "user.UpdateUserLastLogin", `
		UPDATE users SET last_login_at = ? WHERE id = ?
	`, at, userID)
	return err
}

// FindTeamByID loads a team record by ID.
func (r *Repository) FindTeamByID(teamID int64) (TeamRecord, error) {
	var t TeamRecord
	err := dbutil.GetSQL(context.Background(), r.db, "user.FindTeamByID", &t, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE id = ?
		LIMIT 1
	`, teamID)
	return t, err
}

// FindTeamBySlug loads a team record by Slug.
func (r *Repository) FindTeamBySlug(orgName, slug string) (TeamRecord, error) {
	var t TeamRecord
	err := dbutil.GetSQL(context.Background(), r.db, "user.FindTeamBySlug", &t, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE org_name = ? AND slug = ?
		LIMIT 1
	`, orgName, slug)
	return t, err
}

// FindTeamByOrgAndName loads a team record by Org and Name.
func (r *Repository) FindTeamByOrgAndName(orgName, teamName string) (TeamRecord, error) {
	var t TeamRecord
	err := dbutil.GetSQL(context.Background(), r.db, "user.FindTeamByOrgAndName", &t, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE org_name = ? AND name = ? AND active = 1
		LIMIT 1
	`, orgName, teamName)
	return t, err
}

// ListActiveTeamsByOrganization lists active teams for an organization.
func (r *Repository) ListActiveTeamsByOrganization(orgName string) ([]TeamRecord, error) {
	var records []TeamRecord
	err := dbutil.SelectSQL(context.Background(), r.db, "user.ListActiveTeamsByOrganization", &records, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE org_name = ? AND active = 1
		ORDER BY created_at DESC
	`, orgName)
	return records, err
}

// ListActiveTeamsByIDs lists active teams matching IDs.
func (r *Repository) ListActiveTeamsByIDs(teamIDs []int64) ([]TeamRecord, error) {
	if len(teamIDs) == 0 {
		return []TeamRecord{}, nil
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
	var records []TeamRecord
	if err := dbutil.SelectSQL(context.Background(), r.db, "user.ListActiveTeamsByIDs", &records, query, args...); err != nil {
		return nil, err
	}
	return records, nil
}

// CreateTeam inserts a new team record.
func (r *Repository) CreateTeam(orgName, name, slug string, description, icon *string, color, apiKey string, createdAt time.Time) (int64, error) {
	res, err := dbutil.ExecSQL(context.Background(), r.db, "user.CreateTeam", `
		INSERT INTO teams (org_name, name, slug, description, icon, active, color, api_key, created_at)
		VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?)
	`, orgName, name, slug, description, icon, color, apiKey, createdAt)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// FindTeamIDByAPIKey resolves a team ID from its API key.
func (r *Repository) FindTeamIDByAPIKey(ctx context.Context, apiKey string) (int64, error) {
	var teamID int64
	err := dbutil.GetSQL(ctx, r.db, "user.FindTeamIDByAPIKey", &teamID, `
		SELECT id FROM teams WHERE api_key = ? AND active = 1 LIMIT 1
	`, apiKey)
	return teamID, err
}

// FindUserByID loads any user record by ID.
func (r *Repository) FindUserByID(userID int64) (UserRecord, error) {
	var u UserRecord
	err := dbutil.GetSQL(context.Background(), r.db, "user.FindUserByID", &u, `
		SELECT id, email, name, avatar_url, teams, active, last_login_at, created_at
		FROM users
		WHERE id = ?
		LIMIT 1
	`, userID)
	return u, err
}

// ListActiveUsersByTeamIDs lists active users belonging to any given teams.
func (r *Repository) ListActiveUsersByTeamIDs(teamIDs []int64, limit, offset int) ([]UserRecord, error) {
	if len(teamIDs) == 0 {
		return []UserRecord{}, nil
	}

	conditions := make([]string, 0, len(teamIDs))
	args := make([]any, 0, len(teamIDs)+2)
	for _, teamID := range teamIDs {
		conditions = append(conditions, `JSON_CONTAINS(teams, ?)`)
		args = append(args, fmt.Sprintf(`{"team_id":%d}`, teamID))
	}
	args = append(args, limit, offset)

	var records []UserRecord
	err := dbutil.SelectSQL(context.Background(), r.db, "user.ListActiveUsersByTeamIDs", &records, fmt.Sprintf(`
		SELECT id, email, name, avatar_url, teams, active, last_login_at, created_at
		FROM users
		WHERE (%s) AND active = 1
		ORDER BY id
		LIMIT ? OFFSET ?
	`, strings.Join(conditions, " OR ")), args...)
	return records, err
}

// CreateUser inserts a new user record.
func (r *Repository) CreateUser(email, passwordHash, name string, avatarURL, teamsJSON *string, createdAt time.Time) (int64, error) {
	res, err := dbutil.ExecSQL(context.Background(), r.db, "user.CreateUser", `
		INSERT INTO users (email, password_hash, name, avatar_url, teams, active, created_at)
		VALUES (?, ?, ?, ?, ?, 1, ?)
	`, email, NullableString(passwordHash), name, avatarURL, teamsJSON, createdAt)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// UpdateUserProfile updates a user's name and avatar.
func (r *Repository) UpdateUserProfile(userID int64, name, avatarURL *string) error {
	_, err := dbutil.ExecSQL(context.Background(), r.db, "user.UpdateUserProfile", `
		UPDATE users
		SET name = COALESCE(?, name), avatar_url = COALESCE(?, avatar_url)
		WHERE id = ?
	`, name, avatarURL, userID)
	return err
}

// UpdateUserTeams updates the teams associated with a user.
func (r *Repository) UpdateUserTeams(userID int64, teamsJSON string) error {
	_, err := dbutil.ExecSQL(context.Background(), r.db, "user.UpdateUserTeams", `
		UPDATE users SET teams = ? WHERE id = ?
	`, teamsJSON, userID)
	return err
}
