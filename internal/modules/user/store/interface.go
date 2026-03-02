package store

import (
	"time"
)

// AuthUser captures the subset of user fields required for login/token creation.
type AuthUser struct {
	ID           int64
	Email        string
	PasswordHash string
	Name         string
	AvatarURL    string
	Role         string
	TeamsJSON    string
}

// UserTableRepository encapsulates persistence operations for the users table.
type UserTableRepository interface {
	FindByID(userID int64) (map[string]any, error)
	FindActiveByID(userID int64) (map[string]any, error)
	FindActiveByEmail(email string) (AuthUser, error)
	ListActiveByTeamIDs(teamIDs []int64, limit, offset int) ([]map[string]any, error)
	Create(email, passwordHash, name, role, teamsJSON string, createdAt time.Time) (int64, error)
	UpdateLastLogin(userID int64, at time.Time) error
	UpdateProfile(userID int64, name, avatarURL *string, updatedAt time.Time) error
	UpdateTeams(userID int64, teamsJSON string, updatedAt time.Time) error
}

// TeamTableRepository encapsulates persistence operations for the teams table.
type TeamTableRepository interface {
	FindByID(teamID int64) (map[string]any, error)
	FindBySlug(orgID int64, slug string) (map[string]any, error)
	ListActiveByOrganization(orgID int64) ([]map[string]any, error)
	ListActiveByIDs(teamIDs []int64) ([]map[string]any, error)
	Create(orgID int64, name, slug string, description *string, color, apiKey, orgName string, createdAt time.Time) (int64, error)
}

// TableProvider groups table repositories so each table can be swapped independently.
type TableProvider interface {
	Users() UserTableRepository
	Teams() TeamTableRepository
}
