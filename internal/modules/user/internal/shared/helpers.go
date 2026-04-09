package shared

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"

	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
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
		ID:          utils.Int64FromAny(row["id"]),
		Email:       utils.StringFromAny(row["email"]),
		Name:        utils.StringFromAny(row["name"]),
		AvatarURL:   utils.StringFromAny(row["avatar_url"]),
		TeamsJSON:   utils.StringFromAny(row["teams"]),
		Active:      utils.BoolFromAny(row["active"]),
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
	if value := utils.StringFromAny(row["description"]); value != "" {
		description = &value
	}

	return TeamRecord{
		ID:          utils.Int64FromAny(row["id"]),
		OrgName:     utils.StringFromAny(row["org_name"]),
		Name:        utils.StringFromAny(row["name"]),
		Slug:        utils.StringFromAny(row["slug"]),
		Description: description,
		Active:      utils.BoolFromAny(row["active"]),
		Color:       utils.StringFromAny(row["color"]),
		Icon:        utils.StringFromAny(row["icon"]),
		APIKey:      utils.StringFromAny(row["api_key"]),
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
