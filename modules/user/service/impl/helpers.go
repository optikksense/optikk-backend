package impl

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/modules/user/store"
)

func listActiveTeamsForUser(tables store.TableProvider, userID int64) ([]map[string]any, error) {
	rows, err := tables.UserTeams().ListActiveByUser(userID)
	if err != nil {
		return nil, err
	}

	teams := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		teams = append(teams, map[string]any{
			"id":    dbutil.Int64FromAny(row["team_id"]),
			"name":  dbutil.StringFromAny(row["team_name"]),
			"slug":  dbutil.StringFromAny(row["team_slug"]),
			"color": dbutil.StringFromAny(row["team_color"]),
			"role":  dbutil.StringFromAny(row["role"]),
		})
	}
	return teams, nil
}

func buildAuthContextResponse(tables store.TableProvider, userID int64) (map[string]any, error) {
	user, err := tables.Users().FindActiveByID(userID)
	if err != nil || len(user) == 0 {
		return nil, sql.ErrNoRows
	}

	teams, _ := listActiveTeamsForUser(tables, userID)
	var currentTeam map[string]any
	if len(teams) > 0 {
		currentTeam = teams[0]
	}

	return map[string]any{
		"user": map[string]any{
			"id":        dbutil.Int64FromAny(user["id"]),
			"email":     dbutil.StringFromAny(user["email"]),
			"name":      dbutil.StringFromAny(user["name"]),
			"avatarUrl": dbutil.StringFromAny(user["avatar_url"]),
			"role":      dbutil.StringFromAny(user["role"]),
		},
		"teams":       teams,
		"currentTeam": currentTeam,
	}, nil
}

func buildUserResponseByID(tables store.TableProvider, userID int64) (map[string]any, error) {
	user, err := tables.Users().FindByID(userID)
	if err != nil || len(user) == 0 {
		return nil, sql.ErrNoRows
	}

	teams, _ := tables.UserTeams().ListByUser(userID)
	memberships := make([]map[string]any, 0, len(teams))
	for _, team := range teams {
		memberships = append(memberships, map[string]any{
			"teamId":   dbutil.Int64FromAny(team["team_id"]),
			"teamName": dbutil.StringFromAny(team["team_name"]),
			"teamSlug": dbutil.StringFromAny(team["team_slug"]),
			"role":     dbutil.StringFromAny(team["role"]),
		})
	}

	return map[string]any{
		"id":             dbutil.Int64FromAny(user["id"]),
		"organizationId": dbutil.Int64FromAny(user["organization_id"]),
		"email":          dbutil.StringFromAny(user["email"]),
		"name":           dbutil.StringFromAny(user["name"]),
		"avatarUrl":      dbutil.StringFromAny(user["avatar_url"]),
		"role":           dbutil.StringFromAny(user["role"]),
		"active":         dbutil.BoolFromAny(user["active"]),
		"lastLoginAt":    user["last_login_at"],
		"createdAt":      user["created_at"],
		"teams":          memberships,
	}, nil
}

func buildSettingsResponse(tables store.TableProvider, userID int64) (map[string]any, error) {
	row, err := tables.Users().FindActiveByID(userID)
	if err != nil || len(row) == 0 {
		return nil, sql.ErrNoRows
	}

	teams, _ := tables.Teams().ListActiveByOrganization(dbutil.Int64FromAny(row["organization_id"]))
	return map[string]any{
		"userId":      dbutil.Int64FromAny(row["id"]),
		"name":        dbutil.StringFromAny(row["name"]),
		"email":       dbutil.StringFromAny(row["email"]),
		"avatarUrl":   dbutil.StringFromAny(row["avatar_url"]),
		"role":        dbutil.StringFromAny(row["role"]),
		"preferences": map[string]any{},
		"teams":       dbutil.NormalizeRows(teams),
	}, nil
}

func mapInt64(m map[string]any, key string) int64 {
	return dbutil.Int64FromAny(m[key])
}

func generateAPIKey() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
