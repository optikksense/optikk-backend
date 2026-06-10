package user

type LoginRequest struct {
	Email    string `json:"email" binding:"required,email" example:"user@example.com"`
	Password string `json:"password" binding:"required" example:"securePassword123"`
}

type LoginResponse struct {
	AuthContextResponse
	AccessToken string `json:"accessToken"`
}

type CreateTeamRequest struct {
	TeamName    string `json:"team_name" binding:"required"`
	OrgName     string `json:"org_name" binding:"required"`
	Slug        string `json:"slug"`
	Description string `json:"description"`
	Color       string `json:"color"`
	Icon        string `json:"icon"`
}

type CreateUserRequest struct {
	Email     string  `json:"email" binding:"required,email"`
	Name      string  `json:"name" binding:"required"`
	Role      string  `json:"role"`
	Password  string  `json:"password" binding:"required"`
	AvatarURL *string `json:"avatarUrl"`
	TeamIDs   []int64 `json:"teamIds" binding:"required,min=1,dive,gt=0"`
}

type UpdateProfileRequest struct {
	Name      string `json:"name"`
	AvatarURL string `json:"avatarUrl"`
}

type UpdatePreferencesRequest struct {
	Preferences UserPreferences `json:"preferences"`
}
