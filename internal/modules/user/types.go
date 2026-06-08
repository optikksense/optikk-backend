package user

import (
	"time"
)

// TeamMembership represents user's role in a team.
type TeamMembership struct {
	TeamID int64  `json:"team_id"`
	Role   string `json:"role"`
}

// AuthUser is used for credential scanning from the database.
type AuthUser struct {
	ID           int64   `db:"id"`
	Email        string  `db:"email"`
	PasswordHash *string `db:"password_hash"`
	Name         string  `db:"name"`
	AvatarURL    *string `db:"avatar_url"`
	TeamsJSON    *string `db:"teams"`
}

// UserRecord represents a user in the database.
type UserRecord struct {
	ID          int64      `db:"id"`
	Email       string     `db:"email"`
	Name        string     `db:"name"`
	AvatarURL   *string    `db:"avatar_url"`
	TeamsJSON   *string    `db:"teams"`
	Active      bool       `db:"active"`
	LastLoginAt *time.Time `db:"last_login_at"`
	CreatedAt   time.Time  `db:"created_at"`
}

// TeamRecord represents a team in the database.
type TeamRecord struct {
	ID          int64     `db:"id"`
	OrgName     string    `db:"org_name"`
	Name        string    `db:"name"`
	Slug        string    `db:"slug"`
	Description *string   `db:"description"`
	Active      bool      `db:"active"`
	Color       string    `db:"color"`
	Icon        *string   `db:"icon"`
	APIKey      string    `db:"api_key"`
	CreatedAt   time.Time `db:"created_at"`
}
