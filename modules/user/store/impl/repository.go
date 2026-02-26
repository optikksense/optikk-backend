package impl

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	storeinterfaces "github.com/observability/observability-backend-go/modules/user/store/interfaces"
)

// MySQLProvider wires users/teams/user_teams repositories backed by MySQL.
type MySQLProvider struct {
	users     *MySQLUserTable
	teams     *MySQLTeamTable
	userTeams *MySQLUserTeamTable
}

func NewMySQLProvider(db *sql.DB) *MySQLProvider {
	qdb := dbutil.NewMySQLWrapper(db)
	return &MySQLProvider{
		users:     &MySQLUserTable{DB: qdb},
		teams:     &MySQLTeamTable{DB: qdb},
		userTeams: &MySQLUserTeamTable{DB: qdb},
	}
}

func (p *MySQLProvider) Users() storeinterfaces.UserTableRepository {
	return p.users
}

func (p *MySQLProvider) Teams() storeinterfaces.TeamTableRepository {
	return p.teams
}

func (p *MySQLProvider) UserTeams() storeinterfaces.UserTeamTableRepository {
	return p.userTeams
}

type MySQLUserTable struct {
	DB dbutil.Querier
}

func (r *MySQLUserTable) FindByID(userID int64) (map[string]any, error) {
	row, err := dbutil.QueryMap(r.DB, `
		SELECT id, organization_id, email, name, avatar_url, role, active, last_login_at, created_at
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

func (r *MySQLUserTable) FindActiveByID(userID int64) (map[string]any, error) {
	row, err := dbutil.QueryMap(r.DB, `
		SELECT id, organization_id, email, name, avatar_url, role, active, last_login_at, created_at
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

func (r *MySQLUserTable) FindActiveByEmail(email string) (storeinterfaces.AuthUser, error) {
	var user storeinterfaces.AuthUser
	err := r.DB.QueryRow(`
		SELECT id, organization_id, email, COALESCE(password_hash,''), name, COALESCE(avatar_url,''), role
		FROM users
		WHERE email = ? AND active = 1
		LIMIT 1
	`, strings.TrimSpace(email)).Scan(
		&user.ID,
		&user.OrganizationID,
		&user.Email,
		&user.PasswordHash,
		&user.Name,
		&user.AvatarURL,
		&user.Role,
	)
	if err != nil {
		return storeinterfaces.AuthUser{}, err
	}
	return user, nil
}

func (r *MySQLUserTable) ListActiveByOrganization(orgID int64, limit, offset int) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.DB, `
		SELECT id, organization_id, email, name, avatar_url, role, active, last_login_at, created_at
		FROM users
		WHERE organization_id = ? AND active = 1
		ORDER BY id
		LIMIT ? OFFSET ?
	`, orgID, limit, offset)
}

func (r *MySQLUserTable) Create(orgID int64, email, passwordHash, name, role string, createdAt time.Time) (int64, error) {
	res, err := r.DB.Exec(`
		INSERT INTO users (organization_id, email, password_hash, name, role, active, created_at)
		VALUES (?, ?, ?, ?, ?, 1, ?)
	`, orgID, email, nullableStringPtr(&passwordHash), name, role, createdAt)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (r *MySQLUserTable) UpdateLastLogin(userID int64, at time.Time) error {
	_, err := r.DB.Exec(`UPDATE users SET last_login_at = ? WHERE id = ?`, at, userID)
	return err
}

func (r *MySQLUserTable) UpdateProfile(userID int64, name, avatarURL *string, updatedAt time.Time) error {
	_, err := r.DB.Exec(`
		UPDATE users
		SET name = COALESCE(?, name), avatar_url = COALESCE(?, avatar_url), updated_at = ?
		WHERE id = ?
	`, nullableStringPtr(name), nullableStringPtr(avatarURL), updatedAt, userID)
	return err
}

type MySQLTeamTable struct {
	DB dbutil.Querier
}

func (r *MySQLTeamTable) FindByID(teamID int64) (map[string]any, error) {
	row, err := dbutil.QueryMap(r.DB, `
		SELECT id, organization_id, name, slug, description, active, color, icon, api_key, created_at
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

func (r *MySQLTeamTable) FindBySlug(orgID int64, slug string) (map[string]any, error) {
	row, err := dbutil.QueryMap(r.DB, `
		SELECT id, organization_id, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE organization_id = ? AND slug = ?
		LIMIT 1
	`, orgID, slug)
	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		return nil, sql.ErrNoRows
	}
	return row, nil
}

func (r *MySQLTeamTable) ListActiveByOrganization(orgID int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.DB, `
		SELECT id, organization_id, name, slug, description, active, color, icon, api_key, created_at
		FROM teams
		WHERE organization_id = ? AND active = 1
		ORDER BY created_at DESC
	`, orgID)
}

func (r *MySQLTeamTable) ListActiveByUser(userID int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.DB, `
		SELECT t.id, t.organization_id, t.name, t.slug, t.description, t.active, t.color, t.icon, t.api_key, t.created_at, ut.role
		FROM user_teams ut
		JOIN teams t ON t.id = ut.team_id
		WHERE ut.user_id = ? AND t.active = 1
		ORDER BY t.created_at DESC
	`, userID)
}

func (r *MySQLTeamTable) Create(orgID int64, name, slug string, description *string, color, apiKey string, createdAt time.Time) (int64, error) {
	res, err := r.DB.Exec(`
		INSERT INTO teams (organization_id, name, slug, description, active, color, api_key, created_at)
		VALUES (?, ?, ?, ?, 1, ?, ?, ?)
	`, orgID, name, slug, nullableStringPtr(description), color, apiKey, createdAt)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

type MySQLUserTeamTable struct {
	DB dbutil.Querier
}

func (r *MySQLUserTeamTable) Upsert(userID, teamID int64, role string, joinedAt time.Time) error {
	_, err := r.DB.Exec(`
		INSERT INTO user_teams (user_id, team_id, role, joined_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE role = VALUES(role)
	`, userID, teamID, role, joinedAt)
	return err
}

func (r *MySQLUserTeamTable) Delete(userID, teamID int64) error {
	_, err := r.DB.Exec(`DELETE FROM user_teams WHERE user_id = ? AND team_id = ?`, userID, teamID)
	return err
}

func (r *MySQLUserTeamTable) ListByUser(userID int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.DB, `
		SELECT ut.user_id, t.id AS team_id, t.name AS team_name, t.slug AS team_slug, t.color AS team_color, ut.role
		FROM user_teams ut
		JOIN teams t ON t.id = ut.team_id
		WHERE ut.user_id = ?
		ORDER BY ut.joined_at ASC
	`, userID)
}

func (r *MySQLUserTeamTable) ListActiveByUser(userID int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.DB, `
		SELECT ut.user_id, t.id AS team_id, t.name AS team_name, t.slug AS team_slug, t.color AS team_color, ut.role
		FROM user_teams ut
		JOIN teams t ON t.id = ut.team_id
		WHERE ut.user_id = ? AND t.active = 1
		ORDER BY ut.joined_at ASC
	`, userID)
}

func (r *MySQLUserTeamTable) ListByUsers(userIDs []int64, activeTeamsOnly bool) ([]map[string]any, error) {
	if len(userIDs) == 0 {
		return []map[string]any{}, nil
	}

	inClause, args := dbutil.InClauseInt64(userIDs)
	if inClause == "" {
		return []map[string]any{}, nil
	}

	activeFilter := ""
	if activeTeamsOnly {
		activeFilter = " AND t.active = 1"
	}

	query := fmt.Sprintf(`
		SELECT ut.user_id, t.id AS team_id, t.name AS team_name, t.slug AS team_slug, t.color AS team_color, ut.role
		FROM user_teams ut
		JOIN teams t ON t.id = ut.team_id
		WHERE ut.user_id IN %s%s
		ORDER BY ut.user_id ASC, ut.joined_at ASC
	`, inClause, activeFilter)

	return dbutil.QueryMaps(r.DB, query, args...)
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
