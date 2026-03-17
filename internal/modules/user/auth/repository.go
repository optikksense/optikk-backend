package auth

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	usershared "github.com/observability/observability-backend-go/internal/modules/user/internal/shared"
)

type Repository interface {
	FindActiveUserByID(userID int64) (usershared.UserRecord, error)
	FindActiveUserByEmail(email string) (usershared.AuthUser, error)
	FindUserByOAuth(provider, oauthID string) (usershared.AuthUser, error)
	FindTeamByOrgAndName(orgName, teamName string) (usershared.TeamRecord, error)
	ListActiveTeamsByIDs(teamIDs []int64) ([]usershared.TeamRecord, error)
	UpdateUserLastLogin(userID int64, at time.Time) error
	UpdateUserOAuth(userID int64, provider, oauthID string) error
	CreateOAuthUser(email, name, avatarURL, teamsJSON, provider, oauthID string, createdAt time.Time) (int64, error)
}

type MySQLRepository struct {
	db dbutil.Querier
}

func NewRepository(db *sql.DB) *MySQLRepository {
	return &MySQLRepository{db: dbutil.NewMySQLWrapper(db)}
}

func (r *MySQLRepository) FindActiveUserByID(userID int64) (usershared.UserRecord, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, email, name, avatar_url, teams, active, last_login_at, created_at
		FROM users
		WHERE id = ? AND active = 1
		LIMIT 1
	`, userID)
	if err != nil {
		return usershared.UserRecord{}, err
	}
	if len(row) == 0 {
		return usershared.UserRecord{}, sql.ErrNoRows
	}
	return usershared.UserRecordFromMap(row), nil
}

func (r *MySQLRepository) FindActiveUserByEmail(email string) (usershared.AuthUser, error) {
	var user usershared.AuthUser
	err := r.db.QueryRow(`
		SELECT id, email, COALESCE(password_hash,''), name, COALESCE(avatar_url,''), COALESCE(teams, '[]')
		FROM users
		WHERE email = ? AND active = 1
		LIMIT 1
	`, strings.TrimSpace(email)).Scan(
		&user.ID,
		&user.Email,
		&user.PasswordHash,
		&user.Name,
		&user.AvatarURL,
		&user.TeamsJSON,
	)
	if err != nil {
		return usershared.AuthUser{}, err
	}
	return user, nil
}

func (r *MySQLRepository) FindUserByOAuth(provider, oauthID string) (usershared.AuthUser, error) {
	var user usershared.AuthUser
	err := r.db.QueryRow(`
		SELECT id, email, COALESCE(password_hash,''), name, COALESCE(avatar_url,''), COALESCE(teams, '[]')
		FROM users
		WHERE oauth_provider = ? AND oauth_id = ? AND active = 1
		LIMIT 1
	`, provider, oauthID).Scan(
		&user.ID,
		&user.Email,
		&user.PasswordHash,
		&user.Name,
		&user.AvatarURL,
		&user.TeamsJSON,
	)
	if err != nil {
		return usershared.AuthUser{}, err
	}
	return user, nil
}

func (r *MySQLRepository) FindTeamByOrgAndName(orgName, teamName string) (usershared.TeamRecord, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE org_name = ? AND name = ? AND active = 1
		LIMIT 1
	`, orgName, teamName)
	if err != nil {
		return usershared.TeamRecord{}, err
	}
	if len(row) == 0 {
		return usershared.TeamRecord{}, sql.ErrNoRows
	}
	return usershared.TeamRecordFromMap(row), nil
}

func (r *MySQLRepository) ListActiveTeamsByIDs(teamIDs []int64) ([]usershared.TeamRecord, error) {
	if len(teamIDs) == 0 {
		return []usershared.TeamRecord{}, nil
	}
	inClause, args := dbutil.InClauseInt64(teamIDs)
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE id IN %s AND active = 1
		ORDER BY created_at DESC
	`, inClause), args...)
	if err != nil {
		return nil, err
	}
	return usershared.TeamRecordsFromMaps(rows), nil
}

func (r *MySQLRepository) UpdateUserLastLogin(userID int64, at time.Time) error {
	_, err := r.db.Exec(`UPDATE users SET last_login_at = ? WHERE id = ?`, at, userID)
	return err
}

func (r *MySQLRepository) UpdateUserOAuth(userID int64, provider, oauthID string) error {
	_, err := r.db.Exec(`
		UPDATE users SET oauth_provider = ?, oauth_id = ? WHERE id = ?
	`, provider, oauthID, userID)
	return err
}

func (r *MySQLRepository) CreateOAuthUser(email, name, avatarURL, teamsJSON, provider, oauthID string, createdAt time.Time) (int64, error) {
	res, err := r.db.Exec(`
		INSERT INTO users (email, name, avatar_url, teams, oauth_provider, oauth_id, active, created_at)
		VALUES (?, ?, ?, ?, ?, ?, 1, ?)
	`, email, name, nullableStringPtr(&avatarURL), teamsJSON, provider, oauthID, createdAt)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
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
