package identity

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type AuthUser struct {
	ID           int64
	Email        string
	PasswordHash string
	Name         string
	AvatarURL    string
	Role         string
	TeamsJSON    string
}

type Store struct {
	DB dbutil.Querier
}

func NewStore(db *sql.DB) *Store {
	return &Store{DB: dbutil.NewMySQLWrapper(db)}
}

func (r *Store) FindUserByID(userID int64) (map[string]any, error) {
	row, err := dbutil.QueryMap(r.DB, `
		SELECT id, email, name, avatar_url, role, teams, active, last_login_at, created_at
		FROM users
		WHERE id = ?
		LIMIT 1
	`, userID)
	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		return nil, sql.ErrNoRows
	}
	return row, nil
}

func (r *Store) FindActiveUserByID(userID int64) (map[string]any, error) {
	row, err := dbutil.QueryMap(r.DB, `
		SELECT id, email, name, avatar_url, role, teams, active, last_login_at, created_at
		FROM users
		WHERE id = ? AND active = 1
		LIMIT 1
	`, userID)
	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		return nil, sql.ErrNoRows
	}
	return row, nil
}

func (r *Store) FindActiveUserByEmail(email string) (AuthUser, error) {
	var user AuthUser
	err := r.DB.QueryRow(`
		SELECT id, email, COALESCE(password_hash,''), name, COALESCE(avatar_url,''), role, COALESCE(teams, '[]')
		FROM users
		WHERE email = ? AND active = 1
		LIMIT 1
	`, strings.TrimSpace(email)).Scan(
		&user.ID,
		&user.Email,
		&user.PasswordHash,
		&user.Name,
		&user.AvatarURL,
		&user.Role,
		&user.TeamsJSON,
	)
	if err != nil {
		return AuthUser{}, err
	}
	return user, nil
}

func (r *Store) ListActiveUsersByTeamIDs(teamIDs []int64, limit, offset int) ([]map[string]any, error) {
	if len(teamIDs) == 0 {
		return []map[string]any{}, nil
	}

	conditions := make([]string, 0, len(teamIDs))
	args := make([]any, 0, len(teamIDs)+2)
	for _, tid := range teamIDs {
		conditions = append(conditions, `JSON_CONTAINS(teams, ?)`)
		args = append(args, fmt.Sprintf(`{"team_id":%d}`, tid))
	}
	whereClause := strings.Join(conditions, " OR ")
	args = append(args, limit, offset)

	return dbutil.QueryMaps(r.DB, fmt.Sprintf(`
		SELECT id, email, name, avatar_url, role, teams, active, last_login_at, created_at
		FROM users
		WHERE (%s) AND active = 1
		ORDER BY id
		LIMIT ? OFFSET ?
	`, whereClause), args...)
}

func (r *Store) CreateUser(email, passwordHash, name, role, teamsJSON string, createdAt time.Time) (int64, error) {
	res, err := r.DB.Exec(`
		INSERT INTO users (email, password_hash, name, role, teams, active, created_at)
		VALUES (?, ?, ?, ?, ?, 1, ?)
	`, email, nullableStringPtr(&passwordHash), name, role, teamsJSON, createdAt)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (r *Store) UpdateUserLastLogin(userID int64, at time.Time) error {
	_, err := r.DB.Exec(`UPDATE users SET last_login_at = ? WHERE id = ?`, at, userID)
	return err
}

func (r *Store) UpdateUserProfile(userID int64, name, avatarURL *string, updatedAt time.Time) error {
	_, err := r.DB.Exec(`
		UPDATE users
		SET name = COALESCE(?, name), avatar_url = COALESCE(?, avatar_url), updated_at = ?
		WHERE id = ?
	`, nullableStringPtr(name), nullableStringPtr(avatarURL), updatedAt, userID)
	return err
}

func (r *Store) UpdateUserTeams(userID int64, teamsJSON string, updatedAt time.Time) error {
	_, err := r.DB.Exec(`UPDATE users SET teams = ?, updated_at = ? WHERE id = ?`, teamsJSON, updatedAt, userID)
	return err
}

func (r *Store) FindTeamByID(teamID int64) (map[string]any, error) {
	row, err := dbutil.QueryMap(r.DB, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE id = ?
		LIMIT 1
	`, teamID)
	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		return nil, sql.ErrNoRows
	}
	return row, nil
}

func (r *Store) FindTeamBySlug(orgName, slug string) (map[string]any, error) {
	row, err := dbutil.QueryMap(r.DB, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE org_name = ? AND slug = ?
		LIMIT 1
	`, orgName, slug)
	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		return nil, sql.ErrNoRows
	}
	return row, nil
}

func (r *Store) ListActiveTeamsByOrganization(orgName string) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.DB, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE org_name = ? AND active = 1
		ORDER BY created_at DESC
	`, orgName)
}

func (r *Store) ListActiveTeamsByIDs(teamIDs []int64) ([]map[string]any, error) {
	if len(teamIDs) == 0 {
		return []map[string]any{}, nil
	}
	inClause, args := dbutil.InClauseInt64(teamIDs)
	return dbutil.QueryMaps(r.DB, fmt.Sprintf(`
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE id IN %s AND active = 1
		ORDER BY created_at DESC
	`, inClause), args...)
}

func (r *Store) CreateTeam(orgName, name, slug string, description *string, color, apiKey string, createdAt time.Time) (int64, error) {
	res, err := r.DB.Exec(`
		INSERT INTO teams (org_name, name, slug, description, active, color, api_key, created_at)
		VALUES (?, ?, ?, ?, 1, ?, ?, ?)
	`, orgName, name, slug, nullableStringPtr(description), color, apiKey, createdAt)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func nullableStringPtr(v *string) any {
	if v == nil {
		return nil
	}
	if strings.TrimSpace(*v) == "" {
		return nil
	}
	return *v
}
