package helpers

import "fmt"

// ToTeamUUID converts a numeric team ID to the UUID string format
// (e.g. "00000000-0000-0000-0000-000000000001").
func ToTeamUUID(id int64) string {
	if id <= 0 {
		id = 1
	}
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", id%1_000_000_000_000)
}
