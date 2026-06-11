package user

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strings"
)

// ParseTeamMemberships parses the json string of memberships.
func ParseTeamMemberships(raw string) ([]TeamMembership, error) {
	var memberships []TeamMembership
	if raw == "" || raw == "null" {
		return memberships, nil
	}
	err := json.Unmarshal([]byte(raw), &memberships)
	return memberships, err
}

// BuildTeamMembershipsJSON encodes memberships into a json string.
func BuildTeamMembershipsJSON(memberships []TeamMembership) (string, error) {
	if len(memberships) == 0 {
		return "[]", nil
	}
	data, err := json.Marshal(memberships)
	return string(data), err
}

// TeamIDsFromMemberships extracts team ids from a list of memberships.
func TeamIDsFromMemberships(memberships []TeamMembership) []int64 {
	ids := make([]int64, len(memberships))
	for i, membership := range memberships {
		ids[i] = membership.TeamID
	}
	return ids
}

// GenerateAPIKey generates a new secure random API key.
func GenerateAPIKey() (string, error) {
	bytes := make([]byte, 4)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// NullableString maps an empty string to nil.
func NullableString(s string) *string {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}


// ValueOr returns the string value or a default fallback if nil.
func ValueOr(s *string, fallback string) string {
	if s == nil {
		return fallback
	}
	return *s
}
