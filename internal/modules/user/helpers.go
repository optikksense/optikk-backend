package user

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

func parseTeamsJSON(raw string) ([]TeamMembership, error) {
	var memberships []TeamMembership
	if raw == "" || raw == "null" {
		return memberships, nil
	}
	err := json.Unmarshal([]byte(raw), &memberships)
	return memberships, err
}

func buildTeamsJSON(memberships []TeamMembership) (string, error) {
	if len(memberships) == 0 {
		return "[]", nil
	}
	data, err := json.Marshal(memberships)
	return string(data), err
}

func teamIDsFromMemberships(ms []TeamMembership) []int64 {
	ids := make([]int64, len(ms))
	for i, m := range ms {
		ids[i] = m.TeamID
	}
	return ids
}

func listActiveTeamsForUser(store *Store, userID int64) ([]map[string]any, error) {
	user, err := store.FindActiveUserByID(userID)
	if err != nil {
		return nil, err
	}

	teamsJSON := dbutil.StringFromAny(user["teams"])
	memberships, err := parseTeamsJSON(teamsJSON)
	if err != nil {
		return nil, err
	}

	teamIDs := teamIDsFromMemberships(memberships)
	if len(teamIDs) == 0 {
		return []map[string]any{}, nil
	}

	roleByTeamID := make(map[int64]string, len(memberships))
	for _, m := range memberships {
		roleByTeamID[m.TeamID] = m.Role
	}

	rows, err := store.ListActiveTeamsByIDs(teamIDs)
	if err != nil {
		return nil, err
	}

	teams := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		tid := dbutil.Int64FromAny(row["id"])
		teams = append(teams, map[string]any{
			"id":      tid,
			"name":    dbutil.StringFromAny(row["name"]),
			"slug":    dbutil.StringFromAny(row["slug"]),
			"color":   dbutil.StringFromAny(row["color"]),
			"orgName": dbutil.StringFromAny(row["org_name"]),
			"role":    roleByTeamID[tid],
		})
	}
	return teams, nil
}

func buildAuthContextResponse(store *Store, userID int64) (map[string]any, error) {
	user, err := store.FindActiveUserByID(userID)
	if err != nil || len(user) == 0 {
		return nil, sql.ErrNoRows
	}

	teams, _ := listActiveTeamsForUser(store, userID)
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

func buildUserResponseByID(store *Store, userID int64) (map[string]any, error) {
	user, err := store.FindUserByID(userID)
	if err != nil || len(user) == 0 {
		return nil, sql.ErrNoRows
	}

	teamsJSON := dbutil.StringFromAny(user["teams"])
	memberships, _ := parseTeamsJSON(teamsJSON)

	teamIDs := teamIDsFromMemberships(memberships)
	roleByTeamID := make(map[int64]string, len(memberships))
	for _, m := range memberships {
		roleByTeamID[m.TeamID] = m.Role
	}

	var teamDetails []map[string]any
	if len(teamIDs) > 0 {
		rows, _ := store.ListActiveTeamsByIDs(teamIDs)
		teamDetails = make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			tid := dbutil.Int64FromAny(row["id"])
			teamDetails = append(teamDetails, map[string]any{
				"teamId":   tid,
				"teamName": dbutil.StringFromAny(row["name"]),
				"teamSlug": dbutil.StringFromAny(row["slug"]),
				"role":     roleByTeamID[tid],
			})
		}
	}
	if teamDetails == nil {
		teamDetails = []map[string]any{}
	}

	return map[string]any{
		"id":          dbutil.Int64FromAny(user["id"]),
		"email":       dbutil.StringFromAny(user["email"]),
		"name":        dbutil.StringFromAny(user["name"]),
		"avatarUrl":   dbutil.StringFromAny(user["avatar_url"]),
		"role":        dbutil.StringFromAny(user["role"]),
		"active":      dbutil.BoolFromAny(user["active"]),
		"lastLoginAt": user["last_login_at"],
		"createdAt":   user["created_at"],
		"teams":       teamDetails,
	}, nil
}

func buildSettingsResponse(store *Store, userID int64) (map[string]any, error) {
	row, err := store.FindActiveUserByID(userID)
	if err != nil || len(row) == 0 {
		return nil, sql.ErrNoRows
	}

	teamsJSON := dbutil.StringFromAny(row["teams"])
	memberships, _ := parseTeamsJSON(teamsJSON)
	teamIDs := teamIDsFromMemberships(memberships)

	var teams []map[string]any
	if len(teamIDs) > 0 {
		teams, _ = store.ListActiveTeamsByIDs(teamIDs)
	}

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
