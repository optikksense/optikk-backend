package auth

import (
	"database/sql"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	usershared "github.com/Optikk-Org/optikk-backend/internal/modules/user/internal/shared"
)

type Repository interface {
	FindActiveUserByID(userID int64) (usershared.UserRecord, error)
	FindActiveUserByEmail(email string) (usershared.AuthUser, error)
	ListActiveTeamsByIDs(teamIDs []int64) ([]usershared.TeamRecord, error)
	UpdateUserLastLogin(userID int64, at time.Time) error
}

type MySQLRepository struct {
	db dbutil.Querier
}

func NewRepository(db *sql.DB, appConfig registry.AppConfig) *MySQLRepository {
	return &MySQLRepository{
		db: dbutil.NewMySQLWrapper(db),
	}
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

func (r *MySQLRepository) ListActiveTeamsByIDs(teamIDs []int64) ([]usershared.TeamRecord, error) {
	if len(teamIDs) == 0 {
		return []usershared.TeamRecord{}, nil
	}
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE id IN (?) AND active = 1
		ORDER BY created_at DESC
	`, teamIDs)
	if err != nil {
		return nil, err
	}
	return usershared.TeamRecordsFromMaps(rows), nil
}

func (r *MySQLRepository) UpdateUserLastLogin(userID int64, at time.Time) error {
	_, err := r.db.Exec(`UPDATE users SET last_login_at = ? WHERE id = ?`, at, userID)
	return err
}
