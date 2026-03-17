package userpage

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
	AvatarURL   string               `json:"avatarUrl"`
	Active      bool                 `json:"active"`
	LastLoginAt any                  `json:"lastLoginAt"`
	CreatedAt   any                  `json:"createdAt"`
	Teams       []UserListMembership `json:"teams"`
}

type UserResponse struct {
	ID          int64            `json:"id"`
	Email       string           `json:"email"`
	Name        string           `json:"name"`
	AvatarURL   string           `json:"avatarUrl"`
	Active      bool             `json:"active"`
	LastLoginAt any              `json:"lastLoginAt"`
	CreatedAt   any              `json:"createdAt"`
	Teams       []UserTeamDetail `json:"teams"`
}

type ProfileTeam struct {
	ID          int64  `json:"id"`
	OrgName     string `json:"org_name"`
	Name        string `json:"name"`
	Slug        string `json:"slug"`
	Description any    `json:"description"`
	Active      bool   `json:"active"`
	Color       string `json:"color"`
	Icon        string `json:"icon"`
	APIKey      string `json:"api_key"`
	CreatedAt   any    `json:"created_at"`
}

type ProfileResponse struct {
	UserID      int64          `json:"userId"`
	Name        string         `json:"name"`
	Email       string         `json:"email"`
	AvatarURL   string         `json:"avatarUrl"`
	Preferences map[string]any `json:"preferences"`
	Teams       []ProfileTeam  `json:"teams"`
}
