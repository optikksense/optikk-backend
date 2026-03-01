package helpers

import (
	"fmt"
	"strconv"
	"strings"
)

// ToTeamUUID converts a numeric team ID to the UUID string format
// (e.g. "00000000-0000-0000-0000-000000000001").
func ToTeamUUID(id int64) string {
	if id <= 0 {
		id = 1
	}
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", id%1_000_000_000_000)
}

// FromTeamUUID extracts the numeric team ID from a team UUID string.
// Returns 0 if the UUID cannot be parsed.
func FromTeamUUID(uuid string) int64 {
	// UUID format: "00000000-0000-0000-0000-000000000001"
	// The team ID is the last 12 digits after the final hyphen.
	idx := strings.LastIndex(uuid, "-")
	if idx < 0 || idx+1 >= len(uuid) {
		return 0
	}
	id, err := strconv.ParseInt(uuid[idx+1:], 10, 64)
	if err != nil {
		return 0
	}
	return id
}
