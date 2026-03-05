package contracts

type LoginRequest struct {
	Email    string `json:"email" binding:"required" example:"user@example.com"`
	Password string `json:"password" binding:"required" example:"securePassword123"`
}

type UserRequest struct {
	Email    string  `json:"email" binding:"required"`
	Name     string  `json:"name" binding:"required"`
	Role     string  `json:"role"`
	Password string  `json:"password" binding:"required"`
	TeamIDs  []int64 `json:"teamIds" binding:"required,min=1"`
}

type SettingsRequest struct {
	Name        string         `json:"name"`
	AvatarURL   string         `json:"avatarUrl"`
	Preferences map[string]any `json:"preferences"`
}

type TeamRequest struct {
	TeamName    string `json:"team_name" binding:"required"`
	OrgName     string `json:"org_name" binding:"required"`
	Slug        string `json:"slug"`
	Description string `json:"description"`
	Color       string `json:"color"`
}
