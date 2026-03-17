package shared

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

func ParseTeamMemberships(raw string) ([]TeamMembership, error) {
	var memberships []TeamMembership
	if raw == "" || raw == "null" {
		return memberships, nil
	}
	err := json.Unmarshal([]byte(raw), &memberships)
	return memberships, err
}

func BuildTeamMembershipsJSON(memberships []TeamMembership) (string, error) {
	if len(memberships) == 0 {
		return "[]", nil
	}
	data, err := json.Marshal(memberships)
	return string(data), err
}

func TeamIDsFromMemberships(memberships []TeamMembership) []int64 {
	ids := make([]int64, len(memberships))
	for i, membership := range memberships {
		ids[i] = membership.TeamID
	}
	return ids
}

func GenerateAPIKey() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func UserRecordFromMap(row map[string]any) UserRecord {
	return UserRecord{
		ID:          dbutil.Int64FromAny(row["id"]),
		Email:       dbutil.StringFromAny(row["email"]),
		Name:        dbutil.StringFromAny(row["name"]),
		AvatarURL:   dbutil.StringFromAny(row["avatar_url"]),
		TeamsJSON:   dbutil.StringFromAny(row["teams"]),
		Active:      dbutil.BoolFromAny(row["active"]),
		LastLoginAt: row["last_login_at"],
		CreatedAt:   row["created_at"],
	}
}

func UserRecordsFromMaps(rows []map[string]any) []UserRecord {
	records := make([]UserRecord, 0, len(rows))
	for _, row := range rows {
		records = append(records, UserRecordFromMap(row))
	}
	return records
}

func TeamRecordFromMap(row map[string]any) TeamRecord {
	var description *string
	if value := dbutil.StringFromAny(row["description"]); value != "" {
		description = &value
	}

	return TeamRecord{
		ID:          dbutil.Int64FromAny(row["id"]),
		OrgName:     dbutil.StringFromAny(row["org_name"]),
		Name:        dbutil.StringFromAny(row["name"]),
		Slug:        dbutil.StringFromAny(row["slug"]),
		Description: description,
		Active:      dbutil.BoolFromAny(row["active"]),
		Color:       dbutil.StringFromAny(row["color"]),
		Icon:        dbutil.StringFromAny(row["icon"]),
		APIKey:      dbutil.StringFromAny(row["api_key"]),
		CreatedAt:   row["created_at"],
	}
}

func TeamRecordsFromMaps(rows []map[string]any) []TeamRecord {
	records := make([]TeamRecord, 0, len(rows))
	for _, row := range rows {
		records = append(records, TeamRecordFromMap(row))
	}
	return records
}
