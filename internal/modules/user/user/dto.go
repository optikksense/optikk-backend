package userpage

type CreateUserRequest struct {
	Email    string  `json:"email" binding:"required" validate:"required,email"`
	Name     string  `json:"name" binding:"required" validate:"required"`
	Role     string  `json:"role"`
	Password string  `json:"password" binding:"required" validate:"required"`
	TeamIDs  []int64 `json:"teamIds" binding:"required,min=1" validate:"required,min=1,dive,gt=0"`
}

type UpdateProfileRequest struct {
	Name      string `json:"name"`
	AvatarURL string `json:"avatarUrl"`
}

type UpdatePreferencesRequest struct {
	Preferences UserPreferences `json:"preferences"`
}
