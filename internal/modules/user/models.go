package user

type AuthUserSummary struct {
	ID        int64   `json:"id"`
	Email     string  `json:"email"`
	Name      string  `json:"name"`
	AvatarURL *string `json:"avatarUrl"`
}

type AuthTeamSummary struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	Slug    string `json:"slug"`
	Color   string `json:"color"`
	OrgName string `json:"orgName"`
	Role    string `json:"role"`
}

type AuthContextResponse struct {
	User        AuthUserSummary   `json:"user"`
	Teams       []AuthTeamSummary `json:"teams"`
	CurrentTeam *AuthTeamSummary  `json:"currentTeam"`
}

type ValidateTokenResponse struct {
	Valid  bool   `json:"valid"`
	UserID int64  `json:"userId"`
	TeamID int64  `json:"teamId"`
	Role   string `json:"role"`
}

type MessageResponse struct {
	Message string `json:"message"`
}

type TeamResponse struct {
	ID          int64   `json:"id"`
	OrgName     string  `json:"org_name"`
	Name        string  `json:"name"`
	Slug        string  `json:"slug"`
	Description any     `json:"description"`
	Active      bool    `json:"active"`
	Color       string  `json:"color"`
	Icon        *string `json:"icon"`
	APIKey      string  `json:"api_key"`
	CreatedAt   any     `json:"created_at"`
}

type TeamSummary struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	Slug    string `json:"slug"`
	Color   string `json:"color"`
	OrgName string `json:"orgName"`
	Role    string `json:"role"`
}

type UserListMembership struct {
	TeamID int64  `json:"teamId"`
	Role   string `json:"role"`
}

type UserTeamDetail struct {
	TeamID   int64  `json:"teamId"`
	TeamName string `json:"teamName"`
	TeamSlug string `json:"teamSlug"`
	Role     string `json:"role"`
}

type UserListItem struct {
	ID          int64                `json:"id"`
	Email       string               `json:"email"`
	Name        string               `json:"name"`
	AvatarURL   *string              `json:"avatarUrl"`
	Active      bool                 `json:"active"`
	LastLoginAt any                  `json:"lastLoginAt"`
	CreatedAt   any                  `json:"createdAt"`
	Teams       []UserListMembership `json:"teams"`
}

type UserResponse struct {
	ID          int64            `json:"id"`
	Email       string           `json:"email"`
	Name        string           `json:"name"`
	AvatarURL   *string          `json:"avatarUrl"`
	Active      bool             `json:"active"`
	LastLoginAt any              `json:"lastLoginAt"`
	CreatedAt   any              `json:"createdAt"`
	Teams       []UserTeamDetail `json:"teams"`
}

type ProfileTeam struct {
	ID          int64   `json:"id"`
	OrgName     string  `json:"org_name"`
	Name        string  `json:"name"`
	Slug        string  `json:"slug"`
	Description any     `json:"description"`
	Active      bool    `json:"active"`
	Color       string  `json:"color"`
	Icon        *string `json:"icon"`
	APIKey      string  `json:"api_key"`
	CreatedAt   any     `json:"created_at"`
}

type ProfileResponse struct {
	UserID      int64           `json:"userId"`
	Name        string          `json:"name"`
	Email       string          `json:"email"`
	AvatarURL   *string         `json:"avatarUrl"`
	Preferences UserPreferences `json:"preferences"`
	Teams       []ProfileTeam   `json:"teams"`
}
