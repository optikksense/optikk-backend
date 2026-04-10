package team

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	usershared "github.com/Optikk-Org/optikk-backend/internal/modules/user/internal/shared"
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
	db dbutil.Querier
}

func NewRepository(db *sql.DB, appConfig registry.AppConfig) *MySQLRepository {
	return &MySQLRepository{
		db: dbutil.NewMySQLWrapper(db),
	}
}

func (r *MySQLRepository) FindUserByID(userID int64) (usershared.UserRecord, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, email, name, avatar_url, teams, active, last_login_at, created_at
		FROM users
		WHERE id = ?
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

func (r *MySQLRepository) FindTeamByID(teamID int64) (usershared.TeamRecord, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE id = ?
		LIMIT 1
	`, teamID)
	if err != nil {
		return usershared.TeamRecord{}, err
	}
	if len(row) == 0 {
		return usershared.TeamRecord{}, sql.ErrNoRows
	}
	return usershared.TeamRecordFromMap(row), nil
}

func (r *MySQLRepository) FindTeamBySlug(orgName, slug string) (usershared.TeamRecord, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE org_name = ? AND slug = ?
		LIMIT 1
	`, orgName, slug)
	if err != nil {
		return usershared.TeamRecord{}, err
	}
	if len(row) == 0 {
		return usershared.TeamRecord{}, sql.ErrNoRows
	}
	return usershared.TeamRecordFromMap(row), nil
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

func (r *MySQLRepository) ListActiveTeamsByOrganization(orgName string) ([]usershared.TeamRecord, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT id, org_name, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE org_name = ? AND active = 1
		ORDER BY created_at DESC
	`, orgName)
	if err != nil {
		return nil, err
	}
	return usershared.TeamRecordsFromMaps(rows), nil
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

func (r *MySQLRepository) UpdateUserTeams(userID int64, teamsJSON string) error {
	_, err := r.db.Exec(`UPDATE users SET teams = ? WHERE id = ?`, teamsJSON, userID)
	return err
}

func (r *MySQLRepository) CreateTeam(orgName, name, slug string, description *string, color, apiKey string, createdAt time.Time) (int64, error) {
	res, err := r.db.Exec(`
		INSERT INTO teams (org_name, name, slug, description, active, color, api_key, created_at)
		VALUES (?, ?, ?, ?, 1, ?, ?, ?)
	`, orgName, name, slug, nullableStringPtr(description), color, apiKey, createdAt)
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
