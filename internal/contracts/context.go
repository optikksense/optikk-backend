package contracts

import "fmt"

type TenantContext struct {
	TeamID    int64
	UserID    int64
	UserEmail string
	UserRole  string
}

func (t TenantContext) TeamUUID() string {
	id := t.TeamID
	if id <= 0 {
		id = 1
	}
	// Truncate to the last 12 digits to stay within the UUID node field.
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", id%1_000_000_000_000)
}
