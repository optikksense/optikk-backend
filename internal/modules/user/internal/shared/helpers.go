package shared

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strings"
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

// NullableString returns nil when s is empty or whitespace, so MySQL writes NULL
// instead of an empty string. Used for optional columns like password_hash.
func NullableString(s string) *string {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func ValueOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func ValueOr(s *string, fallback string) string {
	if s == nil {
		return fallback
	}
	return *s
}
